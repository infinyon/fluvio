use std::{collections::HashMap, time::Duration};

use fluvio_types::defaults::TLS_CLIENT_SECRET_NAME;
use tracing::{debug, error, info, trace, instrument};
use k8_client::ClientError;
use k8_types::{
    LabelProvider, LabelSelector, TemplateMeta, TemplateSpec,
    core::pod::{
        ConfigMapVolumeSource, ContainerSpec, ImagePullPolicy, KeyToPath, PodSpec, VolumeMount,
        VolumeSpec, SecretVolumeSpec, PodSecurityContext,
    },
    Env,
};

use fluvio_future::{task::spawn, timer::sleep};
use fluvio_stream_dispatcher::store::K8ChangeListener;

use crate::stores::{
    StoreContext,
    connector::{
        ManagedConnectorSpec, ManagedConnectorParameterValueInner, ManagedConnectorStatus,
        ManagedConnectorStatusResolution,
    },
    k8::K8MetaItem,
    MetadataStoreObject,
    actions::WSAction,
};

use crate::k8::objects::managed_connector_deployment::{
    ManagedConnectorDeploymentSpec, K8DeploymentSpec,
};
use crate::k8::objects::spu_k8_config::ScK8Config;

use crate::cli::TlsConfig;

/// Update Statefulset and Service from SPG
pub struct ManagedConnectorDeploymentController {
    connectors: StoreContext<ManagedConnectorSpec>,
    deployments: StoreContext<ManagedConnectorDeploymentSpec>,
    tls_config: Option<TlsConfig>,
    config_ctx: StoreContext<ScK8Config>,
}

impl ManagedConnectorDeploymentController {
    pub fn start(
        connectors: StoreContext<ManagedConnectorSpec>,
        deployments: StoreContext<ManagedConnectorDeploymentSpec>,
        tls_config: Option<TlsConfig>,
        config_ctx: StoreContext<ScK8Config>,
    ) {
        let controller = Self {
            connectors,
            deployments,
            tls_config,
            config_ctx,
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

        let mut connector_prefixes = Vec::new();

        if let Some(config) = self.config_ctx.store().value("fluvio").await {
            let config = config.inner_owned().spec;
            connector_prefixes = config.connector_prefixes;
        }
        let k8_deployment_spec = Self::generate_k8_deployment_spec(
            managed_connector.spec(),
            self.tls_config.as_ref(),
            &connector_prefixes,
        )
        .await;
        trace!(?k8_deployment_spec);
        if let Some(k8_deployment_spec) = k8_deployment_spec {
            let deployment_action = WSAction::Apply(
                MetadataStoreObject::with_spec(key, k8_deployment_spec.into())
                    .with_context(managed_connector.ctx().create_child()),
            );

            debug!(?deployment_action, "applying deployment");

            self.deployments.wait_action(key, deployment_action).await?;
        } else {
            let resolution = ManagedConnectorStatusResolution::Invalid;
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

    const DEFAULT_CONNECTOR_NAME: &'static str = "fluvio-connector";
    pub async fn generate_k8_deployment_spec(
        mc_spec: &ManagedConnectorSpec,
        tls_config: Option<&TlsConfig>,
        allowed_connector_prefix: &[String],
    ) -> Option<K8DeploymentSpec> {
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
        let parameters: Vec<String> = parameters
            .keys()
            .zip(parameters.values())
            .flat_map(|(key, values)| match &values.0 {
                ManagedConnectorParameterValueInner::String(value) => {
                    vec![format!("--{}={}", key.replace('_', "-"), value)]
                }
                ManagedConnectorParameterValueInner::Vec(values) => {
                    let mut args = Vec::new();
                    for value in values.iter() {
                        args.push(format!("--{}={}", key.replace('_', "-"), value))
                    }
                    args
                }
                ManagedConnectorParameterValueInner::Map(map) => {
                    let mut args = Vec::new();
                    for (sub_key, value) in map.iter() {
                        args.push(format!(
                            "--{}={}:{}",
                            key.replace('_', "-"),
                            sub_key.replace('_', "-"),
                            value
                        ));
                    }
                    args
                }
            })
            .collect::<Vec<_>>();

        // Prefixing the args with a "--" passed to the container is needed for an unclear reason.
        let mut args = vec![
            "--".to_string(),
            format!("--fluvio-topic={}", mc_spec.topic),
        ];
        let type_ = &mc_spec.type_;
        let image = Self::get_image(type_, allowed_connector_prefix).await?;

        args.extend(parameters);

        let (image, image_pull_policy) = match mc_spec.version.to_string().as_str() {
            "dev" => (image, ImagePullPolicy::Never),
            "latest" => (format!("{}:latest", image), ImagePullPolicy::Always),
            version => (
                format!("{}:{}", image, version),
                ImagePullPolicy::IfNotPresent,
            ),
        };

        let mut volume_mounts = vec![VolumeMount {
            name: "fluvio-config-volume".to_string(),
            mount_path: "/home/fluvio/.fluvio".to_string(),
            ..Default::default()
        }];

        let mut volumes = vec![config_map_volume_spec];

        if let Some(tls) = tls_config {
            if tls.enable_client_cert {
                volume_mounts.push(VolumeMount {
                    name: "cacert".to_owned(),
                    mount_path: "/var/certs/ca".to_owned(),
                    read_only: Some(true),
                    ..Default::default()
                });
                volumes.push(VolumeSpec {
                    name: "cacert".to_owned(),
                    secret: Some(SecretVolumeSpec {
                        secret_name: "fluvio-ca".to_owned(),
                        ..Default::default()
                    }),
                    ..Default::default()
                });

                volume_mounts.push(VolumeMount {
                    name: "client-tls".to_owned(),
                    mount_path: "/var/certs/client-tls".to_owned(),
                    read_only: Some(true),
                    ..Default::default()
                });

                volumes.push(VolumeSpec {
                    name: "client-tls".to_owned(),
                    secret: Some(SecretVolumeSpec {
                        secret_name: tls
                            .secret_name
                            .clone()
                            .unwrap_or_else(|| TLS_CLIENT_SECRET_NAME.to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
            }
        }
        let secrets = &mc_spec.secrets;
        let env: Vec<Env> = secrets
            .keys()
            .zip(secrets.values())
            .flat_map(|(key, value)| [Env::key_value(key, &(**value).to_string())])
            .collect::<Vec<_>>();

        debug!(
            "Starting connector for image: {:?} with arguments {:?}",
            image, args
        );
        let template = TemplateSpec {
            metadata: Some(TemplateMeta::default().set_labels(vec![
                ("app", Self::DEFAULT_CONNECTOR_NAME),
                ("connectorName", &mc_spec.name),
            ])),
            spec: PodSpec {
                termination_grace_period_seconds: Some(10),
                security_context: Some(PodSecurityContext {
                    fs_group: Some(1000),
                    ..Default::default()
                }),
                containers: vec![ContainerSpec {
                    name: Self::DEFAULT_CONNECTOR_NAME.to_owned(),
                    image: Some(image),
                    image_pull_policy: Some(image_pull_policy),
                    env,
                    volume_mounts,
                    args,
                    ..Default::default()
                }],
                volumes,
                //security_context: spu_k8_config.pod_security_context.clone(),
                //node_selector: Some(spu_pod_config.node_selector.clone()),
                ..Default::default()
            },
        };

        let mut match_labels = HashMap::new();
        match_labels.insert("app".to_owned(), Self::DEFAULT_CONNECTOR_NAME.to_owned());
        match_labels.insert("connectorName".to_owned(), mc_spec.name.clone());

        Some(K8DeploymentSpec {
            template,
            selector: LabelSelector { match_labels },
            ..Default::default()
        })
    }
    pub async fn get_image(type_: &str, allowed_connector_prefix: &[String]) -> Option<String> {
        let image = if type_.starts_with("https://") {
            debug!(
                "Checking 3rd party connector {:?} in allowed_prefixes - {:?}",
                type_, allowed_connector_prefix
            );
            let mut image = None;
            for prefix in allowed_connector_prefix {
                if type_.starts_with(prefix.as_str()) {
                    match ThirdPartyConnectorSpec::from_url(type_).await {
                        Ok(spec) => {
                            debug!("Retrieved third party metadata {:?}", spec);
                            image = Some(spec.image);
                        }
                        Err(e) => {
                            info!("3rd party connector spec failed to retrieve {:?}", e);
                        }
                    }
                    break;
                }
            }
            if let Some(image) = image {
                image
            } else {
                debug!("None of the connector prefixes matched!");
                return None;
            }
        } else {
            format!("infinyon/fluvio-connect-{}", type_)
        };
        Some(image)
    }
}
#[cfg(test)]
mod third_party_connector_tests {
    use super::*;

    #[fluvio_future::test]
    async fn test_authorized_prefix() {
        let image = ManagedConnectorDeploymentController::get_image("foo", &[]).await;
        assert_eq!(image, Some("infinyon/fluvio-connect-foo".to_string()));
    }

    #[fluvio_future::test]
    async fn test_unauthorized_prefix() {
        let image = ManagedConnectorDeploymentController::get_image(
            "https://google.com",
            &["https://yahoo.com".to_string()],
        )
        .await;
        assert_eq!(image, None);
    }

    //#[fluvio_future::test]
    #[allow(unused)]
    async fn test_official_3rd_party_connector() {
        let image = ManagedConnectorDeploymentController::get_image("https://raw.githubusercontent.com/infinyon/fluvio-connectors/main/rust-connectors/utils/test-connector/connector.yaml", &["https://raw.githubusercontent.com/infinyon/fluvio-connectors".to_string()]).await;
        assert_eq!(
            image,
            Some("infinyon/fluvio-connect-test-connector".to_string())
        );
    }
}

#[derive(Default, Debug, Eq, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
pub struct ThirdPartyConnectorSpec {
    pub image: String,
}

impl ThirdPartyConnectorSpec {
    pub async fn from_url(_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // disable third party support
        //let body = surf::get(url).recv_string().await?;
        //Ok(serde_yaml::from_str(&body)?)
        todo!()
    }
}
