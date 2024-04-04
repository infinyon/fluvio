use std::path::Path;
use std::process::Command;
use std::fs::{remove_dir_all, remove_file};

use derive_builder::Builder;
use k8_client::meta_client::MetadataClient;
use tracing::{info, warn, debug, instrument};
use sysinfo::{ProcessExt, System, SystemExt};

use fluvio_command::CommandExt;
use fluvio_types::defaults::SPU_MONITORING_UNIX_SOCKET;

use crate::helm::HelmClient;
use crate::charts::{APP_CHART_NAME, SYS_CHART_NAME};
use crate::progress::ProgressBarFactory;
use crate::render::ProgressRenderer;
use crate::DEFAULT_NAMESPACE;
use crate::error::UninstallError;
use crate::start::local::{DEFAULT_DATA_DIR, LOCAL_CONFIG_PATH};
use anyhow::Result;

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

    /// Used to hide spinner animation for progress updates
    #[builder(default = "true")]
    hide_spinner: bool,
}

impl ClusterUninstallConfig {
    pub fn builder() -> ClusterUninstallConfigBuilder {
        ClusterUninstallConfigBuilder::default()
    }

    pub fn uninstaller(self) -> Result<ClusterUninstaller> {
        ClusterUninstaller::from_config(self)
    }
}

/// Uninstalls different flavors of fluvio
#[derive(Debug)]
pub struct ClusterUninstaller {
    /// Configuration options for this process
    config: ClusterUninstallConfig,
    /// Helm client for performing uninstalls
    helm_client: Option<HelmClient>,
    pb_factory: ProgressBarFactory,
}

impl ClusterUninstaller {
    fn from_config(config: ClusterUninstallConfig) -> Result<Self> {
        let helm_client = if config.uninstall_k8 || config.uninstall_sys {
            Some(HelmClient::new().map_err(UninstallError::HelmError)?)
        } else {
            None
        };

        Ok(ClusterUninstaller {
            helm_client,
            pb_factory: ProgressBarFactory::new(config.hide_spinner),
            config,
        })
    }

    #[instrument(skip(self))]
    pub async fn uninstall(&self) -> Result<()> {
        if self.config.uninstall_k8 {
            self.uninstall_k8().await?;
            if let Err(err) = self.cleanup_k8().await {
                warn!("Cleanup failed: {}", err);
            }
        }
        if self.config.uninstall_local {
            self.uninstall_local().await?;
        }

        if self.config.uninstall_sys {
            self.uninstall_sys().await?;
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn uninstall_k8(&self) -> Result<()> {
        use fluvio_helm::UninstallArg;

        let pb = self.pb_factory.create()?;
        pb.set_message("Uninstalling fluvio kubernetes components");
        let uninstall: UninstallArg = UninstallArg::new(self.config.app_chart_name.to_owned())
            .namespace(self.config.namespace.to_owned())
            .ignore_not_found();
        self.helm_client
            .as_ref()
            .ok_or(UninstallError::Other("helm client undefined".into()))?
            .uninstall(uninstall)
            .map_err(UninstallError::HelmError)?;

        pb.println("Uninstalled fluvio kubernetes components");
        pb.finish_and_clear();

        Ok(())
    }

    #[instrument(skip(self))]
    async fn uninstall_sys(&self) -> Result<()> {
        use fluvio_helm::UninstallArg;

        let pb = self.pb_factory.create()?;
        pb.set_message("Uninstalling Fluvio sys chart");
        self.helm_client
            .as_ref()
            .ok_or(UninstallError::Other("helm client undefined".into()))?
            .uninstall(
                UninstallArg::new(self.config.sys_chart_name.to_owned())
                    .namespace(self.config.namespace.to_owned())
                    .ignore_not_found(),
            )
            .map_err(UninstallError::HelmError)?;
        debug!("fluvio sys chart has been uninstalled");

        pb.set_message("Fluvio System chart has been uninstalled");
        pb.finish_and_clear();

        Ok(())
    }

    async fn uninstall_local(&self) -> Result<()> {
        let pb = self.pb_factory.create()?;
        pb.set_message("Uninstalling fluvio local components");

        let kill_proc = |name: &str, command_args: Option<&[String]>| {
            let mut sys = System::new();
            sys.refresh_processes(); // Only load what we need.
            for process in sys.processes_by_exact_name(name) {
                if let Some(cmd_args) = command_args {
                    // First command is the executable so cut that out.
                    let proc_cmds = &process.cmd()[1..];
                    if cmd_args.len() > proc_cmds.len() {
                        continue; // Ignore procs with less command_args than the target.
                    }
                    if cmd_args.iter().ne(proc_cmds[..cmd_args.len()].iter()) {
                        continue; // Ignore procs which don't match.
                    }
                }
                if !process.kill() {
                    // This will fail if called on a proc running as root, so only log failure.
                    debug!(
                        "Sysinto process.kill() returned false. pid: {}, name: {}: user: {:?}",
                        process.pid(),
                        process.name(),
                        process.user_id(),
                    );
                }
            }
        };
        kill_proc("fluvio", Some(&["cluster".into(), "run".into()]));
        kill_proc("fluvio", Some(&["run".into()]));
        kill_proc("fluvio-run", None);

        fn delete_fs<T: AsRef<Path>>(
            path: Option<T>,
            tag: &'static str,
            is_file: bool,
            pb: Option<&ProgressRenderer>,
        ) {
            match path {
                Some(path) => {
                    let path_ref = path.as_ref();
                    match if is_file {
                        remove_file(path_ref)
                    } else {
                        remove_dir_all(path_ref)
                    } {
                        Ok(_) => {
                            debug!("Removed {}: {}", tag, path_ref.display());
                            if let Some(pb) = pb {
                                pb.println(format!("Removed {}", tag))
                            }
                        }
                        Err(err) => {
                            warn!("{} can't be removed: {}", tag, err);
                            if let Some(pb) = pb {
                                pb.println(format!("{tag}, can't be removed: {err}"))
                            }
                        }
                    }
                }
                None => {
                    warn!("Unable to find {}, cannot remove", tag);
                }
            }
        }

        // delete fluvio file
        debug!("Removing fluvio directory");
        delete_fs(DEFAULT_DATA_DIR.as_ref(), "data dir", false, None);

        // delete local cluster config file
        delete_fs(
            LOCAL_CONFIG_PATH.as_ref(),
            "local cluster config",
            true,
            None,
        );

        // remove monitoring socket
        delete_fs(
            Some(SPU_MONITORING_UNIX_SOCKET),
            "SPU monitoring socket",
            true,
            Some(&pb),
        );

        pb.println("Uninstalled fluvio local components");
        pb.finish_and_clear();

        Ok(())
    }

    /// Clean up k8 objects and secrets created during the installation process
    ///
    /// Ignore any errors, cleanup should be idempotent
    async fn cleanup_k8(&self) -> Result<()> {
        let pb = self.pb_factory.create()?;
        pb.set_message("Cleaning up objects and secrets created during the installation process");
        let ns = &self.config.namespace;

        // delete objects if not removed already
        let _ = self.remove_custom_objects("spugroups", ns, None, false, &pb);
        let _ = self.remove_custom_objects("spus", ns, None, false, &pb);
        let _ = self.remove_custom_objects("topics", ns, None, false, &pb);
        let _ = self.remove_finalizers_for_partitions(ns).await;
        let _ = self.remove_custom_objects("partitions", ns, None, true, &pb);
        let _ = self.remove_custom_objects("statefulset", ns, None, false, &pb);
        let _ =
            self.remove_custom_objects("persistentvolumeclaims", ns, Some("app=spu"), false, &pb);
        let _ = self.remove_custom_objects("tables", ns, None, false, &pb);
        let _ = self.remove_custom_objects("managedconnectors", ns, None, false, &pb);
        let _ = self.remove_custom_objects("derivedstreams", ns, None, false, &pb);
        let _ = self.remove_custom_objects("smartmodules", ns, None, false, &pb);

        // delete secrets
        let _ = self.remove_secrets("fluvio-ca");
        let _ = self.remove_secrets("fluvio-tls");

        pb.println("Objects and secrets have been cleaned up");
        pb.finish_and_clear();
        Ok(())
    }

    /// Remove objects of specified type, namespace
    fn remove_custom_objects(
        &self,
        object_type: &str,
        namespace: &str,
        selector: Option<&str>,
        force: bool,
        pb: &ProgressRenderer,
    ) -> Result<()> {
        pb.set_message(format!("Removing {object_type} objects"));
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
    async fn remove_finalizers_for_partitions(&self, namespace: &str) -> Result<()> {
        use fluvio_controlplane_metadata::partition::PartitionSpec;
        use fluvio_controlplane_metadata::store::k8::K8ExtendedSpec;
        use k8_client::load_and_share;
        use k8_client::meta_client::PatchMergeType::JsonMerge;

        let client = load_and_share()?;

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
    fn remove_secrets(&self, name: &str) -> Result<()> {
        Command::new("kubectl")
            .arg("delete")
            .arg("secret")
            .arg(name)
            .arg("--ignore-not-found=true")
            .output()?;

        Ok(())
    }
}
