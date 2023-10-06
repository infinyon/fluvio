use clap::Parser;
use fluvio::config::ConfigFile;
use tracing::info;

use crate::delete::ClusterUninstallConfig;
use crate::cli::ClusterCliError;

#[derive(Debug, Parser)]
pub struct DeleteOpt {
    #[arg(long, value_name = "Kubernetes namespace")]
    namespace: Option<String>,

    #[arg(long)]
    /// delete system chart
    sys: bool,
}

impl DeleteOpt {
    pub async fn process(self) -> Result<(), ClusterCliError> {
        let mut builder = ClusterUninstallConfig::builder();
        builder.hide_spinner(false);

        if self.sys {
            info!("uninstalling sys chart");
            builder.uninstall_local(false);
            builder.uninstall_k8(false);
            builder.uninstall_sys(true);
        } else {
            let config_file = ConfigFile::load(None)?;
            let current = config_file.config().current_cluster()?;
            let kind = current.kind;
            info!("uninstalling {kind} cluster");

            match kind {
                fluvio::config::ClusterKind::Local => {
                    builder.uninstall_local(true);
                    builder.uninstall_k8(false);
                    builder.uninstall_sys(false);
                }
                fluvio::config::ClusterKind::K8s => {
                    builder.uninstall_local(false);
                    builder.uninstall_k8(true);
                    builder.uninstall_sys(true);
                }
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
