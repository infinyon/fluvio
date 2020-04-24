use std::collections::HashMap;

use log::debug;
use log::error;
use log::info;
use log::trace;
use log::warn;
use futures::stream::StreamExt;

use flv_future_aio::task::spawn;
use k8_client::ClientError;
use k8_metadata::metadata::InputK8Obj;
use k8_metadata::metadata::InputObjectMeta;
use k8_metadata::metadata::K8Watch;
use k8_metadata::core::service::ServiceSpec;
use k8_metadata::core::service::ExternalTrafficPolicy;
use k8_metadata::core::service::LoadBalancerType;
use k8_metadata::core::service::ServicePort;
use k8_metadata::spg::SpuGroupSpec;
use k8_metadata::spg::SpuGroupStatus;
use k8_metadata::spg::SpuEndpointTemplate;
use k8_metadata::metadata::Spec;
use k8_metadata::spu::SpuSpec as K8SpuSpec;
use k8_metadata::spu::SpuType as K8SpuType;
use k8_metadata::spu::IngressPort as K8IngressPort;
use k8_metadata::spu::Endpoint as K8Endpoint;
use k8_metadata::metadata::LabelProvider;
use k8_client::metadata::MetadataClient;
use k8_client::metadata::ApplyResult;
use k8_client::SharedK8Client;
use types::defaults::SPU_PUBLIC_PORT;
use types::defaults::SPU_DEFAULT_NAME;
use types::SpuId;
use flv_sc_core::core::spus::SharedSpuLocalStore;


use crate::cli::TlsConfig;
use super::convert_cluster_to_statefulset;
use super::generate_service;
use super::SpuGroupObj;
use super::SpuValidation;

pub struct SpgOperator {
    client: SharedK8Client,
    spu_store: SharedSpuLocalStore,
    namespace: String,
    tls: Option<TlsConfig>
}

impl SpgOperator {
    pub fn new(
        client: SharedK8Client, 
        namespace: String, 
        spu_store: SharedSpuLocalStore,
        tls: Option<TlsConfig>
    ) -> Self {
        Self {
            client,
            namespace,
            spu_store,
            tls
        }
    }

    pub fn run(self) {
        spawn(self.inner_run());
    }

    async fn inner_run(self) {
        let mut spg_stream = self
            .client
            .watch_stream_since::<SpuGroupSpec,_>(self.namespace.clone(), None);

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

    async fn dispatch_events(
        &self,
        events: Vec<Result<K8Watch<SpuGroupSpec>, ClientError>>,
    ) {
        for event_r in events {
            match event_r {
                Ok(watch_event) => {
                    let result = self.process_event(watch_event).await;
                    match result {
                        Err(err) => error!("error processing k8 spu event: {}", err),
                        _ => {}
                    }
                }
                Err(err) => error!("error in watch item: {}", err),
            }
        }
    }

    async fn process_event(
        &self,
        event: K8Watch<SpuGroupSpec>,
    ) -> Result<(), ClientError> {
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

    async fn apply_spg_changes(&self, spu_group: SpuGroupObj) -> Result<(), ClientError> {
        let spg_name = spu_group.metadata.name.as_ref();

        let spg_spec = &spu_group.spec;

        // ensure we don't have conflict with existing spu group
        if let Some(conflict_id) = spu_group.is_conflict_with(&self.spu_store) {
            warn!(
                "spg group: {} is conflict with existing id: {}",
                spg_name, conflict_id
            );
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
                        .apply_stateful_set(&spu_group, spg_spec, &spg_name, svc_name)
                        .await
                    {
                        error!(
                            "cluster '{}': error applying stateful sets: {}",
                            spg_name, err
                        );
                    }
                }
                Err(err) => {
                    error!(
                        "cluster '{}': error applying spg services: {}",
                        spg_name, err
                    );
                }
            }

            if let Err(err) = self.apply_spus(&spu_group, spg_spec, &spg_name).await {
                error!("cluster '{}': error applying spus: {}", spg_name, err);
            }
        }

        Ok(())
    }

    /// Generate and apply a stateful set for this cluster
    async fn apply_stateful_set(
        &self,
        spu_group: &SpuGroupObj,
        spg_spec: &SpuGroupSpec,
        spg_name: &str,
        spg_svc_name: String,
    ) -> Result<(), ClientError> {
        let input_stateful = convert_cluster_to_statefulset(
            spg_spec,
            &spu_group.metadata,
            spg_name,
            spg_svc_name,
            &self.namespace,
            self.tls.as_ref()
        );
        debug!(
            "cluster '{}': apply statefulset '{}' changes",
            spg_name, input_stateful.metadata.name,
        );

        self.client.apply(input_stateful).await?;

        Ok(())
    }

    /// create SPU crd objects from cluster spec
    async fn apply_spus(
        &self,
        spg_obj: &SpuGroupObj,
        spg_spec: &SpuGroupSpec,
        spg_name: &str,
    ) -> Result<(), ClientError> {
        let replicas = spg_spec.replicas;

        for i in 0..replicas {
            let spu_id = match self.compute_spu_id(spg_spec.min_id(), i) {
                Ok(id) => id,
                Err(err) => {
                    error!("{}", err);
                    continue;
                }
            };

            let spu_name = format!("{}-{}", spg_name, i);
            debug!("generating spu with name: {}", spu_name);

            if let Err(err) = self
                .apply_spu_load_balancers(spg_obj, spg_spec, &spu_name)
                .await
            {
                error!("error trying to create load balancer for spu: {}", err);
            }

            self.apply_spu(spg_obj, spg_spec, spg_name, &spu_name, i, spu_id)
                .await;
        }

        Ok(())
    }

    /// create SPU crd objects from cluster spec
    async fn apply_spu(
        &self,
        k8_group: &SpuGroupObj,
        group_spec: &SpuGroupSpec,
        group_name: &str,
        spu_name: &str,
        _replica_index: u16,
        spu_id: SpuId,
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

        let full_group_name = format!("flv-spg-{}", group_name);
        let full_spu_name = format!("flv-spg-{}", spu_name);
        let spu_spec = K8SpuSpec {
            spu_id: spu_id,
            spu_type: Some(K8SpuType::Managed),
            public_endpoint: K8IngressPort {
                port: spu_public_ep.port,
                encryption: spu_public_ep.encryption,
                ingress: vec![],
            },
            private_endpoint: K8Endpoint {
                host: format!("{}.{}", full_spu_name, full_group_name),
                port: spu_private_ep.port,
                encryption: spu_private_ep.encryption,
            },
            rack: None,
        };

        let owner_ref = k8_metadata.make_owner_reference::<SpuGroupSpec>();
        let input_spu: InputK8Obj<K8SpuSpec> = InputK8Obj {
            api_version: K8SpuSpec::api_version(),
            kind: K8SpuSpec::kind(),
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
    async fn apply_spu_load_balancers(
        &self,
        spg_obj: &SpuGroupObj,
        spg_spec: &SpuGroupSpec,
        spu_name: &str,
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
        public_port.target_port = Some(public_port.port);

        let mut selector = HashMap::new();
        let pod_name = format!("flv-spg-{}", spu_name);
        selector.insert("statefulset.kubernetes.io/pod-name".to_owned(), pod_name);

        let service_spec = ServiceSpec {
            r#type: Some(LoadBalancerType::LoadBalancer),
            external_traffic_policy: Some(ExternalTrafficPolicy::Local),
            selector: Some(selector),
            ports: vec![public_port.clone()],
            ..Default::default()
        };
        let owner_ref = metadata.make_owner_reference::<ServiceSpec>();

        let svc_name = format!("flv-spu-{}", spu_name);

        let input_service: InputK8Obj<ServiceSpec> = InputK8Obj {
            api_version: ServiceSpec::api_version(),
            kind: ServiceSpec::kind(),
            metadata: InputObjectMeta {
                name: svc_name,
                namespace: metadata.namespace().to_string(),
                owner_references: vec![owner_ref],
                ..Default::default()
            }
            .set_labels(vec![("fluvio.io/spu-name", spu_name)]),
            spec: service_spec,
            ..Default::default()
        };

        debug!("spu '{}': enable external services", spu_name);

        self.client.apply(input_service).await
    }

    /// Apply service for statefulsets/group
    /// returns name of the statefulset svc
    async fn apply_statefulset_service(
        &self,
        spg_obj: &SpuGroupObj,
        spg_spec: &SpuGroupSpec,
        spg_name: &str,
    ) -> Result<String, ClientError> {
        let service_name = spg_name.to_owned();
        let service_spec = generate_service(spg_spec, spg_name);
        let metadata = &spg_obj.metadata;
        let owner_ref = metadata.make_owner_reference::<ServiceSpec>();

        let mut labels = HashMap::new();
        labels.insert("app".to_owned(), SPU_DEFAULT_NAME.to_owned());
        labels.insert("group".to_owned(), spg_name.to_owned());

        let svc_name = format!("flv-spg-{}", spg_name);

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
