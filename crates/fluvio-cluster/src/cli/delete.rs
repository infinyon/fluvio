use clap::Parser;

use crate::delete::ClusterUninstallConfig;
use crate::cli::ClusterCliError;

#[derive(Debug, Parser)]
pub struct DeleteOpt {
    #[clap(long, value_name = "Kubernetes namespace")]
    namespace: Option<String>,

    /// Remove only local spu/sc(custom) fluvio installation
    #[clap(long, conflicts_with = "k8", conflicts_with = "sys")]
    local: bool,

    /// Remove only k8 fluvio installation
    #[clap(long, conflicts_with = "local", conflicts_with = "sys")]
    k8: bool,

    #[clap(long, conflicts_with = "k8", conflicts_with = "local")]
    /// delete system chart
    sys: bool,
}

impl DeleteOpt {
    pub async fn process(self) -> Result<(), ClusterCliError> {
        let mut builder = ClusterUninstallConfig::builder();
        builder.hide_spinner(false);

        if self.sys {
            builder.uninstall_local(false);
            builder.uninstall_k8(false);
            builder.uninstall_sys(true);
        } else if self.local {
            builder.uninstall_local(true);
            builder.uninstall_k8(false);
            builder.uninstall_sys(false);
        } else if self.k8 {
            builder.uninstall_local(false);
            builder.uninstall_k8(true);
            builder.uninstall_sys(false);
        } else {
            builder.uninstall_local(true);
            builder.uninstall_k8(true);
            builder.uninstall_sys(true);
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
