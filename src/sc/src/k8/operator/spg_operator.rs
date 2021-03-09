use std::{collections::HashMap, time::Duration};

use fluvio_stream_dispatcher::store::K8ChangeListener;
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::warn;
use tracing::instrument;

use fluvio_types::defaults::SPU_PUBLIC_PORT;
use fluvio_types::defaults::SPU_DEFAULT_NAME;
use fluvio_types::SpuId;
use k8_types::core::service::*;
use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use k8_client::SharedK8Client;
use k8_client::ClientError;

use crate::stores::{StoreContext};
use crate::stores::spg::K8SpuGroupSpec;
use crate::stores::spg::{SpuGroupSpec, SpuGroupStatus};
use crate::stores::spg::SpuEndpointTemplate;
use crate::stores::spu::{SpuSpec};
use crate::cli::TlsConfig;

use super::spg_group::{SpuGroupObj};
use super::ScK8Config;
use super::statefulset::StatefulsetSpec;
use super::spg_service::SpgServiceSpec;

/// reconcile between SPG and Statefulset
pub struct SpgStatefulSetController {
    client: SharedK8Client,
    namespace: String,
    groups: StoreContext<SpuGroupSpec>,
    spus: StoreContext<SpuSpec>,
    statefulsets: StoreContext<StatefulsetSpec>,
    spg_services: StoreContext<SpgServiceSpec>,
    tls: Option<TlsConfig>,
}

impl SpgStatefulSetController {
    pub fn start(
        client: SharedK8Client,
        namespace: String,
        groups: StoreContext<SpuGroupSpec>,
        statefulsets: StoreContext<StatefulsetSpec>,
        spus: StoreContext<SpuSpec>,
        spg_services: StoreContext<SpgServiceSpec>,
        tls: Option<TlsConfig>,
    ) {
        let controller = Self {
            spus,
            groups,
            statefulsets,
            client,
            namespace,
            spg_services,
            tls,
        };

        spawn(controller.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        loop {
            if let Err(err) = self.inner_loop().await {
                error!("error with inner loop: {:#?}", err);
                sleep(Duration::from_secs(300));
            }
        }
    }

    async fn inner_loop(&mut self) -> Result<(), ClientError> {
        use tokio::select;

        let mut spg_listener = self.groups.change_listener();

        loop {
            self.sync_spgs_to_statefulset(&mut spg_listener).await?;

            select! {
                // just in case, we re-sync every 5 minutes
                _ = sleep(Duration::from_secs(60)) => {
                    trace!("timer expired");
                },
                _ = spg_listener.listen() => {
                    trace!("detected spg changes");
                },

            }
        }
    }

    #[instrument(skip(self))]
    /// svc has been changed, update spu
    async fn sync_spgs_to_statefulset(
        &mut self,
        listener: &mut K8ChangeListener<SpuGroupSpec>,
    ) -> Result<(), ClientError> {
        if !listener.has_change() {
            trace!("no spg change, skipping");
            return Ok(());
        }

        let changes = listener.sync_changes().await;
        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();

        debug!(
            "received service changes updates: {},deletes: {},epoch: {}",
            updates.len(),
            deletes.len(),
            epoch,
        );

        // load k8 config,
        let spu_k8_config = ScK8Config::load(&self.client, &self.namespace).await?;

        for group_item in updates.into_iter() {
            let spu_group = SpuGroupObj::new(group_item);

            if let Err(err) = self
                .sync_spg_to_statefulset(spu_group, &spu_k8_config)
                .await
            {
                error!("error applying spg to statefulset {:#?}", err);
            }
        }

        Ok(())
    }

    async fn sync_spg_to_statefulset(
        &mut self,
        spu_group: SpuGroupObj,
        spu_k8_config: &ScK8Config,
    ) -> Result<(), ClientError> {
        let spg_name = spu_group.key();
        let spg_spec = &spu_group.spec;

        // ensure we don't have conflict with existing spu group
        if let Some(conflict_id) = spu_group.is_conflict_with(self.spus.store()).await {
            warn!(conflict_id, "spg is in conflict with existing id");
            let status = SpuGroupStatus::invalid(format!("conflict with: {}", conflict_id));

            self.groups
                .update_status(spg_name.to_owned(), status)
                .await?;
        } else {
            // if we pass this stage, then we reserved id.
            if !spu_group.is_already_valid() {
                let status = SpuGroupStatus::reserved();
                self.groups
                    .update_status(spg_name.to_owned(), status)
                    .await?;
                return Ok(());
            }

            let spg_service_action = spu_group.generate_service();

            trace!("spg_service_actions: {:#?}",spg_service_action);
            self.spg_services.wait_action(spu_group.service_name(), spg_service_action).await?;

            /* 
            let stateful_action =
                spu_group.statefulset_action(&self.namespace, spu_k8_config, self.tls.as_ref());
            */
            /*
            // now apply statefulset
            // ensure we have headless service for statefulset
            match self
                .apply_statefulset_service(spu_group,&spu_k8_config)
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
            */
        }

        Ok(())
    }

    /*
    /// Generate and apply a stateful set for this cluster
    #[instrument(
        skip(self, spu_k8_config),
    )]
    async fn apply_stateful_set(
        &self,
        spu_group: &SpuGroupObj,
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
    */

    /*
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
            let spu_id = self.compute_spu_id(spg_spec.min_id, i);
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
    */

    /*
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
    */

    /// compute spu id with min_id as base
    fn compute_spu_id(&self, min_id: i32, replica_index: u16) -> i32 {
        replica_index as i32 + min_id
    }
}
