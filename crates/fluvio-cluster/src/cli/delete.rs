use std::io::{self, Write};

use clap::Parser;

use fluvio::config::ConfigFile;

use crate::delete::ClusterUninstallConfig;
use crate::cli::ClusterCliError;

#[derive(Debug, Parser)]
pub struct DeleteOpt {
    #[arg(long, value_name = "Kubernetes namespace")]
    namespace: Option<String>,

    /// Remove only local spu/sc(custom) fluvio installation
    #[arg(long, conflicts_with = "k8", conflicts_with = "sys")]
    local: bool,

    /// Remove only k8 fluvio installation
    #[arg(long, conflicts_with = "local", conflicts_with = "sys")]
    k8: bool,

    /// Delete system chart
    #[arg(long, conflicts_with = "k8", conflicts_with = "local")]
    sys: bool,

    /// Do not prompt for confirmation
    #[arg(long)]
    force: bool,
}

impl DeleteOpt {
    pub async fn process(self) -> Result<(), ClusterCliError> {
        if !self.force {
            let config_file = ConfigFile::load_default_or_new()?; // NOTE: Not sure we want `or_new` here
            let (current_cluster, current_profile) = {
                let config = config_file.config();
                (config.current_cluster()?, config.current_profile()?)
            };

            // Prompt for confirmation
            println!(
                "WARNING: You are about to delete {cluster}/{endpoint}. This operation is irreversible \
                and the data stored in your cluster will be permanently lost. \
                \nPlease type the cluster name to confirm: {cluster} <enter> (to confirm) / or CTRL-C (to cancel)",
                cluster= current_profile.cluster,
                endpoint = current_cluster.endpoint,
            );
            let mut confirmation = String::new();
            io::stdout().flush()?;
            io::stdin().read_line(&mut confirmation)?;
            // Remove trailing newline
            let confirmation = confirmation.trim();

            if confirmation != current_profile.cluster {
                println!("Confirmation failed. Aborting.");
                return Ok(());
            }
        }
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
