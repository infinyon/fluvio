use std::process::Command;

use anyhow::bail;
use anyhow::Result;
use clap::Parser;
use tracing::debug;

use fluvio_types::defaults::SPU_MONITORING_UNIX_SOCKET;
use fluvio_command::CommandExt;

use crate::process;
use crate::render::ProgressRenderer;
use crate::cli::ClusterCliError;
use crate::progress::ProgressBarFactory;
use crate::{InstallationType, cli::get_installation_type};

#[derive(Debug, Parser)]
pub struct ShutdownOpt;

impl ShutdownOpt {
    pub async fn process(self) -> Result<()> {
        let pb_factory = ProgressBarFactory::new(false);

        let pb = match pb_factory.create() {
            Ok(pb) => pb,
            Err(_) => {
                return Err(
                    ClusterCliError::Other("Failed to create progress bar".to_string()).into(),
                )
            }
        };
        let (installation_type, config) = get_installation_type()?;
        debug!(?installation_type);

        match installation_type {
            InstallationType::Local | InstallationType::LocalK8 | InstallationType::ReadOnly => {
                Self::shutdown_local(&installation_type, &pb).await?;
            }
            InstallationType::Cloud => {
                let profile = config.config().current_profile_name().unwrap_or("none");
                bail!("'fluvio cluster shutdown does not operate on Infinyon cloud cluster \"{profile}\", use `fluvio cloud ...` commands");
            }
            _ => {
                pb.println("❌ Shutdown is only implemented for local clusters.");
            }
        };

        Ok(())
    }

    async fn shutdown_local(
        installation_type: &InstallationType,
        pb: &ProgressRenderer,
    ) -> Result<()> {
        process::kill_local_processes(pb).await?;

        if let InstallationType::LocalK8 = installation_type {
            let _ = Self::remove_custom_objects("spus", true);
        }

        // remove monitoring socket
        process::delete_fs(
            Some(SPU_MONITORING_UNIX_SOCKET),
            "SPU monitoring socket",
            true,
            Some(pb),
        );

        pb.println("Uninstalled fluvio local components");
        pb.finish_and_clear();

        Ok(())
    }

    /// Remove objects of specified type, namespace
    fn remove_custom_objects(object_type: &str, force: bool) -> Result<()> {
        let mut cmd = Command::new("kubectl");
        cmd.arg("delete");
        cmd.arg(object_type);
        cmd.arg("--all");
        if force {
            cmd.arg("--force");
        }
        cmd.result()?;

        Ok(())
    }
}
