use anyhow::bail;
use anyhow::Result;
use clap::Parser;
use tracing::debug;

use crate::{InstallationType, cli::get_installation_type};
use crate::delete::ClusterUninstallConfig;
use crate::cli::ClusterCliError;

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
}

impl DeleteOpt {
    pub async fn process(self) -> Result<()> {
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
