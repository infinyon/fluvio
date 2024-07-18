use std::fs::remove_file;
use std::process::Command;

use anyhow::bail;
use anyhow::Result;
use clap::Parser;
use tracing::debug;
use sysinfo::System;

use fluvio_types::defaults::SPU_MONITORING_UNIX_SOCKET;
use fluvio_command::CommandExt;

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
                Self::kill_local_processes(&installation_type, &pb).await?;
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

    async fn kill_local_processes(
        installation_type: &InstallationType,
        pb: &ProgressRenderer,
    ) -> Result<()> {
        pb.set_message("Uninstalling fluvio local components");

        let kill_proc = |name: &str, command_args: Option<&[String]>| {
            let mut sys = System::new();
            sys.refresh_processes(); // Only load what we need.
            for process in sys.processes_by_exact_name(name) {
                if let Some(cmd_args) = command_args {
                    let proc_cmds = process.cmd();
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

        if let InstallationType::LocalK8 = installation_type {
            let _ = Self::remove_custom_objects("spus", true);
        }

        // remove monitoring socket
        match remove_file(SPU_MONITORING_UNIX_SOCKET) {
            Ok(_) => {
                pb.println(format!(
                    "Removed spu monitoring socket: {SPU_MONITORING_UNIX_SOCKET}"
                ));
            }
            Err(err) => {
                pb.println(format!(
                    "SPU monitoring socket  {SPU_MONITORING_UNIX_SOCKET}, can't be removed: {err}"
                ));
            }
        }

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
