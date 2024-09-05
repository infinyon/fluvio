use anyhow::bail;
use anyhow::Result;
use clap::Parser;
use dialoguer::Input;
use tracing::debug;
use dialoguer::theme::ColorfulTheme;

use crate::{InstallationType, cli::get_installation_type};
use crate::delete::ClusterUninstallConfig;
use crate::cli::{ClusterCliError, ConfigFile};

#[derive(Debug, Parser)]
pub struct DeleteOpt {
    #[arg(long, value_name = "Kubernetes namespace")]
    namespace: Option<String>,

    /// Remove only k8 fluvio installation
    #[arg(long = "k8", conflicts_with = "sys_only")]
    k8_only: bool,

    #[arg(long = "sys", conflicts_with = "k8_only")]
    /// Remove system chart only
    sys_only: bool,

    /// Do not prompt for confirmation
    #[arg(long)]
    force: bool,
}

impl DeleteOpt {
    pub async fn process(self) -> Result<()> {
        let config_file = ConfigFile::load_default_or_new()?;

        let (current_cluster, current_profile) = {
            let config = config_file.config();
            (config.current_cluster()?, config.current_profile()?)
        };

        if !self.force {
            let mut user_input: String = Input::with_theme(&ColorfulTheme::default()).with_prompt(format!(
                "WARNING: You are about to delete {cluster}/{endpoint}. This operation is irreversible \
                and the data stored in your cluster will be permanently lost. \
                \nPlease type the cluster name to confirm: {cluster} <enter> (to confirm) / or CTRL-C (to cancel)",
                cluster= current_profile.cluster,
                endpoint = current_cluster.endpoint,
            )).interact_text()?;

            user_input = user_input.trim().to_string();

            if user_input != current_profile.cluster {
                println!("Confirmation failed. Aborting.");
                return Ok(());
            }
        }

        println!(
            "Deleting {cluster}/{endpoint}",
            cluster = current_profile.cluster,
            endpoint = current_cluster.endpoint,
        );

        let mut builder = ClusterUninstallConfig::builder();
        builder.hide_spinner(false);

        if self.sys_only {
            builder.uninstall_local(false);
            builder.uninstall_k8(false);
            builder.uninstall_sys(true);
        } else if self.k8_only {
            builder.uninstall_local(false);
            builder.uninstall_k8(true);
            builder.uninstall_sys(false);
        } else {
            let (installation_type, config) = get_installation_type()?;
            debug!(?installation_type);
            match installation_type {
                InstallationType::K8 => {
                    builder.uninstall_local(false);
                    builder.uninstall_k8(true);
                    builder.uninstall_sys(true);
                }
                InstallationType::LocalK8 => {
                    builder.uninstall_local(true);
                    builder.uninstall_k8(true);
                    builder.uninstall_sys(true);
                }
                InstallationType::Local | InstallationType::ReadOnly => {
                    builder.uninstall_local(true);
                    builder.uninstall_k8(false);
                    builder.uninstall_sys(false);
                }
                InstallationType::Cloud => {
                    let profile = config.config().current_profile_name().unwrap_or("none");
                    bail!(
                        "Error: delete command is not supported for cloud profile \"{profile}\"\n    \
                        try 'fluvio cloud cluster delete' or 'fluvio profile switch'"
                    );
                }
                other => bail!("Error: delete command is not supported for {other}"),
            }
        }

        if let Some(namespace) = self.namespace {
            builder.namespace(namespace);
        }

        let uninstaller = builder
            .build()
            .map_err(|err| ClusterCliError::Other(format!("builder error: {err:#?}")))?
            .uninstaller()?;

        uninstaller.uninstall().await?;

        Ok(())
    }
}
