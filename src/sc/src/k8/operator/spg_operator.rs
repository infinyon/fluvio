use std::{collections::HashMap, time::Duration};

use fluvio_stream_dispatcher::{actions::WSAction, store::K8ChangeListener};
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::warn;
use tracing::instrument;

use fluvio_types::SpuId;
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
                error!("error with spg loop loop: {:#?}", err);
                debug!("sleeping 1 miniute to try again");
                sleep(Duration::from_secs(60)).await;
            }
        }
    }

    #[instrument(skip(self),name="SpgLoop")]
    async fn inner_loop(&mut self) -> Result<(), ClientError> {
        use tokio::select;

        let mut spg_listener = self.groups.change_listener();

        loop {
            self.sync_spgs_to_statefulset(&mut spg_listener).await?;

            select! {
                // just in case, we re-sync every 5 minutes
                _ = sleep(Duration::from_secs(60)) => {
                    debug!("resync timer expired");
                },
                _ = spg_listener.listen() => {
                    debug!("detected spg changes");
                },

            }
        }
    }

    
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

            self.sync_spg_to_statefulset(spu_group, &spu_k8_config)
                .await?
        }

        Ok(())
    }

    #[instrument(skip(self,spu_k8_config))]
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

            let (spg_service_key,spg_service_action) = spu_group.generate_service();

            trace!("spg_service_actions: {:#?}",spg_service_action);
            self.spg_services.wait_action(&spg_service_key, spg_service_action).await?;

            
            let (stateful_key,stateful_action) =
                spu_group.statefulset_action(&self.namespace, spu_k8_config, self.tls.as_ref());

            self.statefulsets.wait_action(&stateful_key,stateful_action).await?;
            
            self.apply_spus(&spu_group,&spu_k8_config).await?;
        }

        Ok(())
    }

    
    
    /// create SPU crd objects from cluster spec
    //#[instrument(skip(self, spg_obj, spg_spec, spg_name))]
    async fn apply_spus(
        &self,
        spg_obj: &SpuGroupObj,
        spu_k8_config: &ScK8Config,
    ) -> Result<(), ClientError> {

        let spec = spg_obj.spec();
        let replicas = spec.replicas;

        for i in 0..replicas {
            let spu_id = self.compute_spu_id(spec.min_id, i);
            let spu_name = format!("{}-{}", spg_obj.key(), i);
            debug!(%spu_name,"generating spu with name");

            self.apply_spu(spg_obj,&spu_name, i, spu_id)
                .await;

            self.apply_spu_load_balancers(spg_obj,&spu_name, &spu_k8_config)
                .await?;
        }

        Ok(())
    }
    

    
    /// create SPU crd objects from cluster spec
    #[instrument(
        skip(self, _replica_index, id),
        fields(
            replica_index = _replica_index,
            spu_id = id,
        )
    )]
    async fn apply_spu(
        &self,
        spg_obj: &SpuGroupObj,
        spu_name: &str,
        _replica_index: u16,
        id: SpuId,
    ) {

    
        let spu_private_ep = SpuEndpointTemplate::default_private();
        let spu_public_ep = if let Some(ref ep) = SpuEndpointTemplate::default_public();

        let full_group_name = format!("fluvio-spg-{}", spg_obj.key());
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

        let action = WSAction::Apply(
            MetadataStoreObject::with_spec(spu_name, spu_spec)
                .with_context(spg_obj.ctx().create_child()));

        trace!("spu action: {:#?}", action);

        self.spus.wait_action(&stateful_key,stateful_action).await?;
    }

    
    async fn apply_spu_load_balancers(
        &self,
        spg_obj: &SpuGroupObj,
        spu_name: &str,
        spu_k8_config: &ScK8Config,
    ) -> Result<(), ClientError> {

        
        let metadata = &spg_obj.metadata;

       
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

    

    /// compute spu id with min_id as base
    fn compute_spu_id(&self, min_id: i32, replica_index: u16) -> i32 {
        replica_index as i32 + min_id
    }
}
