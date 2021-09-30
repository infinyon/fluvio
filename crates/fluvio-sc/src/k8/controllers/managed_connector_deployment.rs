use std::{collections::HashMap, time::Duration};

use tracing::{debug, error, trace, instrument};
use k8_client::ClientError;
use k8_types::{
    LabelSelector, TemplateSpec, TemplateMeta,
    core::pod::{
        PodSpec, ContainerSpec, VolumeMount, ConfigMapVolumeSource, KeyToPath, VolumeSpec,
    },
    LabelProvider,
};

use fluvio_future::{task::spawn, timer::sleep};
use fluvio_stream_dispatcher::store::K8ChangeListener;

use crate::stores::{
    StoreContext,
    connector::{ManagedConnectorSpec, ManagedConnectorStatus, ManagedConnectorStatusResolution},
    k8::K8MetaItem,
    MetadataStoreObject,
    actions::WSAction,
};

use crate::k8::objects::managed_connector_deployment::{
    ManagedConnectorDeploymentSpec, K8DeploymentSpec,
};

/// Update Statefulset and Service from SPG
pub struct ManagedConnectorDeploymentController {
    namespace: String,
    connectors: StoreContext<ManagedConnectorSpec>,
    deployments: StoreContext<ManagedConnectorDeploymentSpec>,
}

impl ManagedConnectorDeploymentController {
    pub fn start(
        namespace: String,
        connectors: StoreContext<ManagedConnectorSpec>,
        deployments: StoreContext<ManagedConnectorDeploymentSpec>,
    ) {
        let controller = Self {
            namespace,
            connectors,
            deployments,
        };

        spawn(controller.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        loop {
            if let Err(err) = self.inner_loop().await {
                error!("error with managed connector loop: {:#?}", err);
                debug!("sleeping 1 miniute to try again");
                sleep(Duration::from_secs(60)).await;
            }
        }
    }

    #[instrument(skip(self), name = "ManagedConnectorDeploymentController")]
    async fn inner_loop(&mut self) -> Result<(), ClientError> {
        use tokio::select;

        let mut connector_listener = self.connectors.change_listener();
        let _ = connector_listener.wait_for_initial_sync().await;

        let mut deployment_listener = self.deployments.change_listener();
        let _ = deployment_listener.wait_for_initial_sync().await;

        self.sync_connectors_to_deployments(&mut connector_listener)
            .await?;

        loop {
            select! {
                _ = connector_listener.listen() => {
                    debug!("detected connector changes");
                    self.sync_connectors_to_deployments(&mut connector_listener).await?;
                },
                _ = deployment_listener.listen() => {
                    self.sync_deployments_to_connector(&mut deployment_listener).await?;
                }
            }
        }
    }

    async fn sync_deployments_to_connector(
        &mut self,
        listener: &mut K8ChangeListener<ManagedConnectorDeploymentSpec>,
    ) -> Result<(), ClientError> {
        if !listener.has_change() {
            trace!("no managed connector change, skipping");
            return Ok(());
        }
        let changes = listener.sync_changes().await;
        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();

        debug!(
            "received managed connector deployment changes updates: {},deletes: {},epoch: {}",
            updates.len(),
            deletes.len(),
            epoch,
        );
        for mc_deployment in updates.into_iter() {
            let key = mc_deployment.key();
            let deployment_status = mc_deployment.status();
            let ready_replicas = deployment_status.0.ready_replicas;
            let resolution = match ready_replicas {
                Some(ready_replicas) if ready_replicas > 0 => {
                    ManagedConnectorStatusResolution::Running
                }
                _ => ManagedConnectorStatusResolution::Failed,
            };

            let connector_status = ManagedConnectorStatus {
                resolution,
                ..Default::default()
            };
            self.connectors
                .update_status(key.to_string(), connector_status.clone())
                .await?;
        }
        Ok(())
    }

    /// svc has been changed, update spu
    async fn sync_connectors_to_deployments(
        &mut self,
        listener: &mut K8ChangeListener<ManagedConnectorSpec>,
    ) -> Result<(), ClientError> {
        if !listener.has_change() {
            trace!("no managed connector change, skipping");
            return Ok(());
        }

        let changes = listener.sync_changes().await;
        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();

        debug!(
            "received managed connector changes updates: {},deletes: {},epoch: {}",
            updates.len(),
            deletes.len(),
            epoch,
        );

        for mc_item in updates.into_iter() {
            self.sync_connector_to_deployment(mc_item).await?
        }

        Ok(())
    }

    #[instrument(skip(self, managed_connector))]
    async fn sync_connector_to_deployment(
        &mut self,
        managed_connector: MetadataStoreObject<ManagedConnectorSpec, K8MetaItem>,
    ) -> Result<(), ClientError> {
        let key = managed_connector.key();

        let k8_deployment_spec =
            Self::generate_k8_deployment_spec(managed_connector.spec(), &self.namespace, key);
        trace!(?k8_deployment_spec);
        let deployment_action = WSAction::Apply(
            MetadataStoreObject::with_spec(key, k8_deployment_spec.into())
                .with_context(managed_connector.ctx().create_child()),
        );

        debug!(?deployment_action, "applying deployment");

        self.deployments.wait_action(key, deployment_action).await?;

        Ok(())
    }

    const DEFAULT_CONNECTOR_NAME: &'static str = "fluvio-connector";
    pub fn generate_k8_deployment_spec(
        mc_spec: &ManagedConnectorSpec,
        _namespace: &str,
        _name: &str,
    ) -> K8DeploymentSpec {
        let image = format!("infinyon/fluvio-connect-{}", mc_spec.type_);
        debug!("Starting connector for image: {:?}", image);

        let config_map_volume_spec = VolumeSpec {
            name: "fluvio-config-volume".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some("fluvio-config-map".to_string()),
                items: Some(vec![KeyToPath {
                    key: "fluvioClientConfig".to_string(),
                    path: "config".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let parameters = &mc_spec.parameters;
        let args: Vec<String> = parameters
            .keys()
            .zip(parameters.values())
            .flat_map(|(key, value)| [key.clone(), value.clone()])
            .collect::<Vec<_>>();
        let template = TemplateSpec {
            metadata: Some(
                TemplateMeta::default().set_labels(vec![("app", Self::DEFAULT_CONNECTOR_NAME)]),
            ),
            spec: PodSpec {
                termination_grace_period_seconds: Some(10),
                containers: vec![ContainerSpec {
                    name: Self::DEFAULT_CONNECTOR_NAME.to_owned(),
                    image: Some(image),
                    image_pull_policy: Some("IfNotPresent".to_string()),
                    /*
                    env, // TODO
                    */
                    volume_mounts: vec![VolumeMount {
                        name: "fluvio-config-volume".to_string(),
                        mount_path: "/home/fluvio/.fluvio".to_string(),
                        ..Default::default()
                    }],
                    args,
                    ..Default::default()
                }],
                volumes: vec![config_map_volume_spec],
                //security_context: spu_k8_config.pod_security_context.clone(),
                //node_selector: Some(spu_pod_config.node_selector.clone()),
                ..Default::default()
            },
        };

        let mut match_labels = HashMap::new();
        match_labels.insert("app".to_owned(), Self::DEFAULT_CONNECTOR_NAME.to_owned());

        K8DeploymentSpec {
            template,
            selector: LabelSelector { match_labels },
            ..Default::default()
        }
    }
}
