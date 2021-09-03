use std::collections::HashMap;
use std::{time::Duration};

use fluvio_stream_dispatcher::{store::K8ChangeListener};
use k8_types::LabelSelector;
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::warn;
use tracing::instrument;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use k8_client::ClientError;

use crate::stores::{StoreContext};
use crate::stores::managed_connector::ManagedConnectorSpec;


//use crate::k8::objects::deployment::DeploymentStatus;
use crate::k8::objects::managed_connector_deployment::ManagedConnectorDeploymentSpec;

use crate::stores::k8::K8MetaItem;
use fluvio_controlplane_metadata::store::MetadataStoreObject;
use crate::k8::objects::managed_connector_deployment::K8DeploymentSpec;

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

        self.sync_connectors_to_deployments(&mut connector_listener)
            .await?;

        loop {
            select! {
                _ = connector_listener.listen() => {
                    debug!("detected connector changes");
                    self.sync_connectors_to_deployments(&mut connector_listener).await?;
                },
            }
        }
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
            Self::generate_k8_deployment_spec(&managed_connector.spec(), &self.namespace, key);
        trace!(?k8_deployment_spec);
        use crate::stores::MetadataStoreObject;
        use crate::stores::actions::WSAction;

        let deployment_action = WSAction::Apply(
            MetadataStoreObject::with_spec(key, k8_deployment_spec.into())
                .with_context(managed_connector.ctx().create_child()),
        );

        debug!(?deployment_action, "applying deployment");

        self.deployments
            .wait_action(&key, deployment_action)
            .await?;

        Ok(())
    }
    const DEFAULT_CONNECTOR_NAME: &'static str = "fluvio-connector";
    pub fn generate_k8_deployment_spec(
        mc_spec: &ManagedConnectorSpec,
        _namespace: &str,
        _name: &str,
    ) -> K8DeploymentSpec {
        use k8_types::{
            TemplateSpec, TemplateMeta,
            core::pod::{PodSpec, ContainerSpec},
            LabelProvider,
        };
        let image = format!("infinyon/fluvio-connect-{}", mc_spec.config.type_);
        debug!("STARTING CONNECTOR FOR IMAGE {:?}", image);
        let args = &mc_spec.config.args;
        let args : Vec<String> = args.keys().zip(args.values()).flat_map(|(key, value)| [key.clone(), value.clone()]).collect::<Vec<_>>();


        let template = TemplateSpec {
            metadata: Some(
                TemplateMeta::default().set_labels(vec![("app", Self::DEFAULT_CONNECTOR_NAME)]),
            ),
            spec: PodSpec {
                termination_grace_period_seconds: Some(10),
                containers: vec![ContainerSpec {
                    name: Self::DEFAULT_CONNECTOR_NAME.to_owned(),
                    image: Some(image.clone()),
                    image_pull_policy: Some("Never".to_string()),
                    /*
                    env,
                    */
                    args,
                    ..Default::default()
                }],
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
