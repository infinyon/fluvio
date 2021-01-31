use std::collections::HashMap;
use std::sync::Arc;

use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;
use tracing::instrument;
use futures_util::stream::StreamExt;

use fluvio_types::defaults::SPU_PUBLIC_PORT;
use fluvio_types::defaults::SPU_DEFAULT_NAME;
use fluvio_types::SpuId;
use k8_types::*;
use k8_types::core::service::*;
use fluvio_future::task::spawn;
use k8_client::ClientError;
use k8_client::meta_client::{MetadataClient, ApplyResult};
use k8_client::SharedK8Client;

use crate::cli::TlsConfig;
use crate::stores::spg::K8SpuGroupSpec;
use crate::stores::spg::SpuGroupStatus;
use crate::stores::spg::SpuEndpointTemplate;
use crate::stores::spu::*;
use crate::stores::spu::SpuAdminStore;
use crate::core::SharedContext;

use super::conversion::{convert_cluster_to_statefulset,generate_service};
use super::spg_group::{SpuGroupObj,SpuValidation};
use super::ScK8Config;

pub struct SpgOperator {
    client: SharedK8Client,
    spu_store: Arc<SpuAdminStore>,
    namespace: String,
    tls: Option<TlsConfig>,
}

impl SpgOperator {
    pub fn new(
        client: SharedK8Client,
        namespace: String,
        ctx: SharedContext,
        tls: Option<TlsConfig>,
    ) -> Self {
        Self {
            client,
            namespace,
            spu_store: ctx.spus().store().clone(),
            tls,
        }
    }

    pub fn run(self) {
        spawn(self.inner_run());
    }

    async fn inner_run(self) {
        let mut spg_stream = self
            .client
            .watch_stream_since::<K8SpuGroupSpec, _>(self.namespace.clone(), None);

        info!("starting spg operator with namespace: {}", self.namespace);
        while let Some(result) = spg_stream.next().await {
            match result {
                Ok(events) => {
                    self.dispatch_events(events).await;
                }
                Err(err) => error!("error occurred during watch: {}", err),
            }
        }

        debug!("spg operator finished");
    }

    #[instrument(
        skip(self, events),
        fields(namespace = &*self.namespace)
    )]
    async fn dispatch_events(&self, events: Vec<Result<K8Watch<K8SpuGroupSpec>, ClientError>>) {
        for event_r in events {
            match event_r {
                Ok(watch_event) => {
                    let result = self.process_event(watch_event).await;
                    if let Err(err) = result {
                        error!("error processing k8 spu event: {}", err)
                    }
                }
                Err(err) => error!("error in watch item: {}", err),
            }
        }
    }

    async fn process_event(&self, event: K8Watch<K8SpuGroupSpec>) -> Result<(), ClientError> {
        trace!("watch event: {:#?}", event);
        match event {
            K8Watch::ADDED(obj) => {
                debug!("watch: ADD event -> apply");
                self.apply_spg_changes(obj).await
            }
            K8Watch::MODIFIED(obj) => {
                debug!("watch: MOD event -> apply");
                self.apply_spg_changes(obj).await
            }
            K8Watch::DELETED(_) => {
                debug!("RCVD watch item DEL event -> deleted");
                Ok(())
            }
        }
    }

    #[instrument(
        skip(self, spu_group),
        fields(spg_name = &*spu_group.metadata.name),
    )]
    async fn apply_spg_changes(&self, spu_group: SpuGroupObj) -> Result<(), ClientError> {
        let spg_name = spu_group.metadata.name.as_ref();

        let spg_spec = &spu_group.spec;

        let spu_k8_config = ScK8Config::load(&self.client, &self.namespace).await?;

        // ensure we don't have conflict with existing spu group
        if let Some(conflict_id) = spu_group.is_conflict_with(&self.spu_store).await {
            warn!(conflict_id, "spg is in conflict with existing id");
            let status = SpuGroupStatus::invalid(format!("conflict with: {}", conflict_id));

            let k8_status_change = spu_group.as_status_update(status);
            if let Err(err) = self.client.update_status(&k8_status_change).await {
                error!("error: {} updating status: {:#?}", err, k8_status_change)
            }
        } else {
            // if we pass this stage, then we reserved id.
            if !spu_group.is_already_valid() {
                let status_change = spu_group.as_status_update(SpuGroupStatus::reserved());
                if let Err(err) = self.client.update_status(&status_change).await {
                    error!("error: {} updating status: {:#?}", err, status_change)
                }
            }
            // ensure we have headless service for statefulset
            match self
                .apply_statefulset_service(&spu_group, spg_spec, &spg_name)
                .await
            {
                Ok(svc_name) => {
                    if let Err(err) = self
                        .apply_stateful_set(
                            &spu_group,
                            spg_spec,
                            &spg_name,
                            svc_name,
                            &spu_k8_config,
                        )
                        .await
                    {
                        error!("error applying stateful sets: {}", err);
                    }
                }
                Err(err) => {
                    error!("error applying spg services: {}", err);
                }
            }

            if let Err(err) = self
                .apply_spus(&spu_group, spg_spec, &spg_name, &spu_k8_config)
                .await
            {
                error!("error applying spus: {}", err);
            }
        }

        Ok(())
    }

    /// Generate and apply a stateful set for this cluster
    #[instrument(
        skip(self, spu_group, spg_spec, spg_svc_name),
        fields(spg_svc_name = &*spg_svc_name),
    )]
    async fn apply_stateful_set(
        &self,
        spu_group: &SpuGroupObj,
        spg_spec: &K8SpuGroupSpec,
        spg_name: &str,
        spg_svc_name: String,
        spu_k8_config: &ScK8Config,
    ) -> Result<(), ClientError> {
        let input_stateful = convert_cluster_to_statefulset(
            spg_spec,
            &spu_group.metadata,
            spg_name,
            spg_svc_name,
            &self.namespace,
            spu_k8_config,
            self.tls.as_ref(),
        );
        debug!(
            "apply statefulset '{}' changes",
            input_stateful.metadata.name
        );

        trace!("statefulset: {:#?}", input_stateful);

        self.client.apply(input_stateful).await?;

        Ok(())
    }

    /// create SPU crd objects from cluster spec
    //#[instrument(skip(self, spg_obj, spg_spec, spg_name))]
    async fn apply_spus(
        &self,
        spg_obj: &SpuGroupObj,
        spg_spec: &K8SpuGroupSpec,
        spg_name: &str,
        spu_k8_config: &ScK8Config,
    ) -> Result<(), ClientError> {
        let replicas = spg_spec.replicas;

        for i in 0..replicas {
            let spu_id = match self.compute_spu_id(spg_spec.min_id, i) {
                Ok(id) => id,
                Err(err) => {
                    error!("{}", err);
                    continue;
                }
            };

            let spu_name = format!("{}-{}", spg_name, i);
            debug!("generating spu with name: {}", spu_name);

            self.apply_spu(spg_obj, spg_spec, spg_name, &spu_name, i, spu_id)
                .await;

            if let Err(err) = self
                .apply_spu_load_balancers(spg_obj, spg_spec, &spu_name, &spu_k8_config)
                .await
            {
                error!("error trying to create load balancer for spu: {}", err);
            }
        }

        Ok(())
    }

    /// create SPU crd objects from cluster spec
    #[instrument(
        skip(self, k8_group, group_spec, _replica_index, id),
        fields(
            namespace = k8_group.metadata.namespace(),
            replica_index = _replica_index,
            spu_id = id,
        )
    )]
    async fn apply_spu(
        &self,
        k8_group: &SpuGroupObj,
        group_spec: &K8SpuGroupSpec,
        group_name: &str,
        spu_name: &str,
        _replica_index: u16,
        id: SpuId,
    ) {
        let k8_metadata = &k8_group.metadata;
        let k8_namespace = k8_metadata.namespace();
        let spu_template = &group_spec.template.spec;

        let spu_private_ep = if let Some(ref ep) = &spu_template.private_endpoint {
            ep.clone()
        } else {
            SpuEndpointTemplate::default_private()
        };
        let spu_public_ep = if let Some(ref ep) = &spu_template.public_endpoint {
            ep.clone()
        } else {
            SpuEndpointTemplate::default_public()
        };

        let full_group_name = format!("fluvio-spg-{}", group_name);
        let full_spu_name = format!("fluvio-spg-{}", spu_name);
        let spu_spec = SpuSpec {
            id,
            spu_type: SpuType::Managed,
            public_endpoint: IngressPort {
                port: spu_public_ep.port,
                encryption: spu_public_ep.encryption,
                ingress: vec![],
            },
            private_endpoint: Endpoint {
                host: format!("{}.{}", full_spu_name, full_group_name),
                port: spu_private_ep.port,
                encryption: spu_private_ep.encryption,
            },
            rack: None,
        };

        let owner_ref = k8_metadata.make_owner_reference::<K8SpuGroupSpec>();
        let input_spu: InputK8Obj<SpuSpec> = InputK8Obj {
            api_version: SpuSpec::api_version(),
            kind: SpuSpec::kind(),
            metadata: InputObjectMeta {
                name: spu_name.to_string(),
                namespace: k8_namespace.to_owned(),
                owner_references: vec![owner_ref],
                ..Default::default()
            },
            spec: spu_spec,
            ..Default::default()
        };

        debug!("spu '{}': apply changes", spu_name);

        if let Err(err) = self.client.apply(input_spu).await {
            error!("spu '{}': {}", spu_name, err);
        }
    }

    /// Apply external svc for each SPU
    /// This is owned by group svc
    #[instrument(skip(self, spg_obj, spg_spec))]
    async fn apply_spu_load_balancers(
        &self,
        spg_obj: &SpuGroupObj,
        spg_spec: &K8SpuGroupSpec,
        spu_name: &str,
        spu_k8_config: &ScK8Config,
    ) -> Result<ApplyResult<ServiceSpec>, ClientError> {
        let metadata = &spg_obj.metadata;

        let spu_template = &spg_spec.template.spec;
        let mut public_port = ServicePort {
            port: spu_template
                .public_endpoint
                .as_ref()
                .map(|t| t.port)
                .unwrap_or(SPU_PUBLIC_PORT),
            ..Default::default()
        };
        public_port.target_port = Some(TargetPort::Number(public_port.port));

        let mut selector = HashMap::new();
        let pod_name = format!("fluvio-spg-{}", spu_name);
        selector.insert("statefulset.kubernetes.io/pod-name".to_owned(), pod_name);

        let mut service_spec = ServiceSpec {
            selector: Some(selector),
            ports: vec![public_port.clone()],
            ..Default::default()
        };

        spu_k8_config.apply_service(&mut service_spec);

        let owner_ref = metadata.make_owner_reference::<ServiceSpec>();

        let svc_name = format!("fluvio-spu-{}", spu_name);

        let input_service: InputK8Obj<ServiceSpec> = InputK8Obj {
            api_version: ServiceSpec::api_version(),
            kind: ServiceSpec::kind(),
            metadata: InputObjectMeta {
                name: svc_name,
                namespace: metadata.namespace().to_string(),
                owner_references: vec![owner_ref],
                annotations: spu_k8_config.lb_service_annotations.clone(),
                ..Default::default()
            }
            .set_labels(vec![("fluvio.io/spu-name", spu_name)]),
            spec: service_spec,
            ..Default::default()
        };

        debug!("enable external services");
        self.client.apply(input_service).await
    }

    /// Apply service for statefulsets/group
    /// returns name of the statefulset svc
    async fn apply_statefulset_service(
        &self,
        spg_obj: &SpuGroupObj,
        spg_spec: &K8SpuGroupSpec,
        spg_name: &str,
    ) -> Result<String, ClientError> {
        let service_name = spg_name.to_owned();
        let service_spec = generate_service(spg_spec, spg_name);
        let metadata = &spg_obj.metadata;
        let owner_ref = metadata.make_owner_reference::<ServiceSpec>();

        let mut labels = HashMap::new();
        labels.insert("app".to_owned(), SPU_DEFAULT_NAME.to_owned());
        labels.insert("group".to_owned(), spg_name.to_owned());

        let svc_name = format!("fluvio-spg-{}", spg_name);

        let input_service: InputK8Obj<ServiceSpec> = InputK8Obj {
            api_version: ServiceSpec::api_version(),
            kind: ServiceSpec::kind(),
            metadata: InputObjectMeta {
                name: svc_name.clone(),
                namespace: metadata.namespace().to_string(),
                labels,
                owner_references: vec![owner_ref],
                ..Default::default()
            },
            spec: service_spec,
            ..Default::default()
        };

        debug!(
            "spg '{}': apply service '{}' changes",
            spg_name, service_name,
        );

        self.client.apply(input_service).await?;

        Ok(svc_name)
    }

    /// compute spu id with min_id as base
    fn compute_spu_id(&self, min_id: i32, replica_index: u16) -> Result<i32, ClientError> {
        Ok(replica_index as i32 + min_id)
    }
}
