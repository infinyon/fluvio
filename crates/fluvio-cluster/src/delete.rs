use std::process::Command;
use std::fs::remove_dir_all;

use derive_builder::Builder;
use tracing::{info, warn, debug, instrument};

use fluvio_command::CommandExt;

use crate::helm::HelmClient;
use crate::charts::{APP_CHART_NAME, SYS_CHART_NAME};
use crate::{DEFAULT_NAMESPACE};
use crate::error::UninstallError;
use crate::ClusterError;
use crate::start::local::DEFAULT_DATA_DIR;

/// Uninstalls different flavors of fluvio
#[derive(Builder, Debug)]
pub struct ClusterUninstallConfig {
    #[builder(setter(into), default = "DEFAULT_NAMESPACE.to_string()")]
    namespace: String,

    #[builder(default = "false")]
    uninstall_sys: bool,

    /// by default, only k8 is uninstalled
    #[builder(default = "true")]
    uninstall_k8: bool,

    #[builder(default = "false")]
    uninstall_local: bool,

    #[builder(default = "APP_CHART_NAME.to_string()")]
    app_chart_name: String,

    #[builder(default = "SYS_CHART_NAME.to_string()")]
    sys_chart_name: String,
}

impl ClusterUninstallConfig {
    pub fn builder() -> ClusterUninstallConfigBuilder {
        ClusterUninstallConfigBuilder::default()
    }

    pub fn uninstaller(self) -> Result<ClusterUninstaller, ClusterError> {
        ClusterUninstaller::from_config(self)
    }
}

/// Uninstalls different flavors of fluvio
#[derive(Debug)]
pub struct ClusterUninstaller {
    /// Configuration options for this process
    config: ClusterUninstallConfig,
    /// Helm client for performing uninstalls
    helm_client: HelmClient,
}

impl ClusterUninstaller {
    fn from_config(config: ClusterUninstallConfig) -> Result<Self, ClusterError> {
        Ok(ClusterUninstaller {
            config,
            helm_client: HelmClient::new().map_err(UninstallError::HelmError)?,
        })
    }

    #[instrument(skip(self))]
    pub async fn uninstall(&self) -> Result<(), ClusterError> {
        info!("Removing fluvio cluster");

        if self.config.uninstall_k8 {
            self.uninstall_k8().await?;
        } else if self.config.uninstall_local {
            self.uninstall_local().await?;
        } else if self.config.uninstall_sys {
            self.uninstall_sys().await?;
        }

        self.cleanup().await;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn uninstall_k8(&self) -> Result<(), ClusterError> {
        use fluvio_helm::UninstallArg;

        info!("Removing fluvio cluster at k8");
        let uninstall = UninstallArg::new(self.config.app_chart_name.to_owned())
            .namespace(self.config.namespace.to_owned())
            .ignore_not_found();
        self.helm_client
            .uninstall(uninstall)
            .map_err(UninstallError::HelmError)?;

        Ok(())
    }

    /// Uninstall fluvio system chart
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterUninstaller;
    /// let uninstaller = ClusterUninstaller::new()
    ///     .with_namespace("my-namespace")
    ///     .build().unwrap();
    /// uninstaller.uninstall_sys();
    /// ```
    #[instrument(skip(self))]
    async fn uninstall_sys(&self) -> Result<(), ClusterError> {
        use fluvio_helm::UninstallArg;

        self.helm_client
            .uninstall(
                UninstallArg::new(self.config.sys_chart_name.to_owned())
                    .namespace(self.config.namespace.to_owned())
                    .ignore_not_found(),
            )
            .map_err(UninstallError::HelmError)?;
        debug!("fluvio sys chart has been uninstalled");

        Ok(())
    }

    /// Uninstall local fluvio system chart
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterUninstaller;
    /// let uninstaller = ClusterUninstaller::new()
    ///     .with_name("my-namespace")
    ///     .build().unwrap();
    /// uninstaller.uninstall_local();
    /// ```
    async fn uninstall_local(&self) -> Result<(), ClusterError> {
        info!("Removing local cluster");
        Command::new("pkill")
            .arg("-f")
            .arg("fluvio cluster run")
            .output()
            .map_err(UninstallError::IoError)?;
        Command::new("pkill")
            .arg("-f")
            .arg("fluvio run")
            .output()
            .map_err(UninstallError::IoError)?;
        Command::new("pkill")
            .arg("-f")
            .arg("fluvio-run")
            .output()
            .map_err(UninstallError::IoError)?;

        // delete fluvio file
        debug!("Removing fluvio directory");
        match &*DEFAULT_DATA_DIR {
            Some(data_dir) => match remove_dir_all(data_dir) {
                Ok(_) => {
                    debug!("Removed data dir: {}", data_dir.display());
                }
                Err(err) => {
                    warn!("fluvio dir can't be removed: {}", err);
                }
            },
            None => {
                warn!("Unable to find data dir, cannot remove");
            }
        }

        Ok(())
    }

    /// Clean up objects and secrets created during the installation process
    ///
    /// Ignore any errors, cleanup should be idempotent
    async fn cleanup(&self) {
        let ns = &self.config.namespace;

        // delete objects if not removed already
        let _ = self.remove_custom_objects("spugroups", ns, None, false);
        let _ = self.remove_custom_objects("spus", ns, None, false);
        let _ = self.remove_custom_objects("topics", ns, None, false);
        let _ = self.remove_finalizers_for_partitions(ns).await;
        let _ = self.remove_custom_objects("partitions", ns, None, true);
        let _ = self.remove_custom_objects("statefulset", ns, None, false);
        let _ = self.remove_custom_objects("persistentvolumeclaims", ns, Some("app=spu"), false);
        let _ = self.remove_custom_objects("tables", ns, None, false);
        let _ = self.remove_custom_objects("managedconnectors", ns, None, false);
        let _ = self.remove_custom_objects("derivedstreams", ns, None, false);
        let _ = self.remove_custom_objects("smartmodules", ns, None, false);

        // delete secrets
        let _ = self.remove_secrets("fluvio-ca");
        let _ = self.remove_secrets("fluvio-tls");
    }

    /// Remove objects of specified type, namespace
    fn remove_custom_objects(
        &self,
        object_type: &str,
        namespace: &str,
        selector: Option<&str>,
        force: bool,
    ) -> Result<(), UninstallError> {
        let mut cmd = Command::new("kubectl");
        cmd.arg("delete");
        cmd.arg(object_type);
        cmd.arg("--namespace");
        cmd.arg(namespace);
        if force {
            cmd.arg("--force");
        }
        if let Some(label) = selector {
            info!(
                "deleting label '{}' object {} in: {}",
                label, object_type, namespace
            );
            cmd.arg("--selector").arg(label);
        } else {
            info!("deleting all {} in: {}", object_type, namespace);
            cmd.arg("--all");
        }
        cmd.result()?;

        Ok(())
    }

    /// in order to remove partitions, finalizers need to be cleared
    #[instrument(skip(self))]
    async fn remove_finalizers_for_partitions(
        &self,
        namespace: &str,
    ) -> Result<(), UninstallError> {
        use fluvio_controlplane_metadata::partition::PartitionSpec;
        use fluvio_controlplane_metadata::store::k8::K8ExtendedSpec;
        use k8_client::load_and_share;
        use k8_metadata_client::MetadataClient;
        use k8_metadata_client::PatchMergeType::JsonMerge;

        let client = load_and_share().map_err(UninstallError::K8ClientError)?;

        let partitions = client
            .retrieve_items::<<PartitionSpec as K8ExtendedSpec>::K8Spec, _>(namespace)
            .await?;

        if !partitions.items.is_empty() {
            let finalizer: serde_json::Value = serde_json::from_str(
                r#"
                    {
                        "metadata": {
                            "finalizers":null
                        }
                    }
                "#,
            )
            .expect("finalizer");

            for partition in partitions.items.into_iter() {
                client
                    .patch::<<PartitionSpec as K8ExtendedSpec>::K8Spec, _>(
                        &partition.metadata.as_input(),
                        &finalizer,
                        JsonMerge,
                    )
                    .await?;
            }
        }

        // find all partitions

        Ok(())
    }

    /// Remove K8 secret
    fn remove_secrets(&self, name: &str) -> Result<(), UninstallError> {
        Command::new("kubectl")
            .arg("delete")
            .arg("secret")
            .arg(name)
            .arg("--ignore-not-found=true")
            .output()?;

        Ok(())
    }
}
