use std::time::Duration;
use std::process::Command;
use std::fs::remove_dir_all;

use tracing::{info, warn, debug, instrument};
use k8_client::ClientError as K8ClientError;
use k8_client::{load_and_share, SharedK8Client};
use k8_client::http::status::StatusCode;
use k8_types::Spec;
use k8_types::core::pod::PodSpec;
use fluvio_future::timer::sleep;
use fluvio_command::CommandExt;

use crate::helm::HelmClient;
use crate::{DEFAULT_CHART_APP_REPO, DEFAULT_NAMESPACE, DEFAULT_CHART_SYS_REPO};
use crate::error::UninstallError;
use crate::ClusterError;
use crate::start::local::DEFAULT_DATA_DIR;

/// Uninstalls different flavors of fluvio
#[derive(Debug)]
pub struct ClusterUninstallerBuilder {
    /// The namespace to uninstall
    namespace: String,
    /// name of the chart repo
    name: String,
    /// retries
    retry_count: u16,
}

impl ClusterUninstallerBuilder {
    /// Creates a `ClusterUninstaller` with the current configuration.
    ///
    /// This may fail if there is a problem `helm` executable on the local system.
    ///
    /// # Example
    ///
    /// The simplest flow to create a `ClusterUninstaller` looks like the
    /// following:
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterUninstaller;
    /// let installer = ClusterUninstaller::new()
    ///     .build()
    ///     .expect("should create ClusterUnInstaller");
    /// ```
    pub fn build(self) -> Result<ClusterUninstaller, ClusterError> {
        Ok(ClusterUninstaller {
            config: self,
            helm_client: HelmClient::new().map_err(UninstallError::HelmError)?,
        })
    }
    /// Sets the Kubernetes namespace.
    ///
    /// The default namespace is "default".
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterUninstaller;
    /// let uninstaller = ClusterUninstaller::new()
    ///     .with_namespace("my-namespace");
    /// ```
    pub fn with_namespace<S: Into<String>>(mut self, namespace: S) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Sets the chart repo name.
    ///
    /// The default name is "fluvio".
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterUninstaller;
    /// let uninstaller = ClusterUninstaller::new()
    ///     .with_name("my-name");
    /// ```
    pub fn with_name<S: Into<String>>(mut self, name: S) -> Self {
        self.name = name.into();
        self
    }
    /// Sets the chart repo name.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterUninstaller;
    /// let uninstaller = ClusterUninstaller::new()
    ///     .with_retries(2u16);
    /// ```
    pub fn with_retries(mut self, retry_count: u16) -> Self {
        self.retry_count = retry_count;
        self
    }
}

/// Uninstalls different flavors of fluvio
#[derive(Debug)]
pub struct ClusterUninstaller {
    /// Configuration options for this process
    config: ClusterUninstallerBuilder,
    /// Helm client for performing uninstalls
    helm_client: HelmClient,
}

impl ClusterUninstaller {
    /// Creates a cluster unistaller.
    ///
    /// This will initialise defaults and, a helm client to perform various helm operations.
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> ClusterUninstallerBuilder {
        ClusterUninstallerBuilder {
            namespace: DEFAULT_NAMESPACE.to_string(),
            name: DEFAULT_CHART_APP_REPO.to_string(),
            retry_count: 10,
        }
    }

    /// Uninstall fluvio
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterUninstaller;
    /// let uninstaller = ClusterUninstaller::new()
    ///     .with_namespace("my-namespace")
    ///     .build().unwrap();
    /// uninstaller.uninstall();
    /// ```
    #[instrument(skip(self))]
    pub async fn uninstall(&self) -> Result<(), ClusterError> {
        use fluvio_helm::UninstallArg;

        info!("Removing kubernetes cluster");
        let uninstall = UninstallArg::new(self.config.name.to_owned())
            .namespace(self.config.namespace.to_owned())
            .ignore_not_found();
        self.helm_client
            .uninstall(uninstall)
            .map_err(UninstallError::HelmError)?;

        let client = load_and_share().map_err(UninstallError::K8ClientError)?;

        self.wait_for_delete::<PodSpec>(client, "fluvio-sc").await?;
        self.cleanup().await;

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
    pub async fn uninstall_sys(&self) -> Result<(), ClusterError> {
        use fluvio_helm::UninstallArg;

        self.helm_client
            .uninstall(
                UninstallArg::new(DEFAULT_CHART_SYS_REPO.to_owned())
                    .namespace(self.config.namespace.to_owned())
                    .ignore_not_found(),
            )
            .map_err(UninstallError::HelmError)?;
        debug!("fluvio sys chart has been uninstalled");
        self.cleanup().await;

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
    pub async fn uninstall_local(&self) -> Result<(), ClusterError> {
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
        self.cleanup().await;
        Ok(())
    }

    /// Clean up objects and secrets created during the installation process
    ///
    /// Ignore any errors, cleanup should be idempotent
    async fn cleanup(&self) {
        let ns = &self.config.namespace;

        // delete objects
        let _ = self.remove_custom_objects("spugroups", ns, None);
        let _ = self.remove_custom_objects("spus", ns, None);
        let _ = self.remove_custom_objects("topics", ns, None);
        let _ = self.remove_custom_objects("persistentvolumeclaims", ns, Some("app=spu"));

        // delete secrets
        let _ = self.remove_secrets("fluvio-ca");
        let _ = self.remove_secrets("fluvio-tls");

        let _ = self.remove_partitions(ns).await;
        let _ = self.remove_custom_objects("partitions", ns, None);
    }

    /// Remove objects of specified type, namespace
    fn remove_custom_objects(
        &self,
        object_type: &str,
        namespace: &str,
        selector: Option<&str>,
    ) -> Result<(), UninstallError> {
        let mut cmd = Command::new("kubectl");
        cmd.arg("delete");
        cmd.arg(object_type);
        cmd.arg("--namespace");
        cmd.arg(namespace);
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
    async fn remove_partitions(&self, namespace: &str) -> Result<(), UninstallError> {
        use fluvio_controlplane_metadata::partition::PartitionSpec;
        use fluvio_controlplane_metadata::store::k8::K8ExtendedSpec;
        use k8_client::meta_client::MetadataClient;
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

    async fn wait_for_delete<S: Spec>(
        &self,
        client: SharedK8Client,
        prefix: &str,
    ) -> Result<(), UninstallError> {
        use k8_client::meta_client::MetadataClient;
        let mut success = false;
        for i in 0..self.config.retry_count {
            debug!("checking to see if {} is deleted, count: {}", S::label(), i);
            let pod_exists = match client
                .retrieve_items::<S, _>(self.config.namespace.clone())
                .await
            {
                Ok(pods) => {
                    pods.items
                        .iter()
                        .filter(|i| i.metadata.name.starts_with(prefix))
                        .count()
                        > 0
                }
                Err(err) => match err {
                    K8ClientError::Client(status) if status == StatusCode::NOT_FOUND => true,
                    _ => {
                        return Err(UninstallError::Other(err.to_string()));
                    }
                },
            };
            if pod_exists {
                debug!("sc {} still exists, sleeping 10 second", S::label());
                sleep(Duration::from_millis(10000)).await;
            } else {
                debug!("no sc {} found, can proceed to setup ", S::label());
                success = true;
                break;
            }
        }
        if !success {
            return Err(UninstallError::Other(
                "Waiting for too long, failing".to_string(),
            ));
        }
        Ok(())
    }
}
