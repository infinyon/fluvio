use std::fs::remove_file;
use std::process::Command;

use clap::Parser;
use tracing::debug;
use sysinfo::{ProcessExt, System, SystemExt};

use fluvio_types::defaults::SPU_MONITORING_UNIX_SOCKET;
use fluvio_command::CommandExt;

use crate::render::ProgressRenderer;
use crate::cli::ClusterCliError;
use crate::progress::ProgressBarFactory;
use crate::{ClusterError, UninstallError};

#[derive(Debug, Parser)]
pub struct ShutdownOpt {
    /// shutdown local spu/sc
    #[arg(long)]
    local: bool,

    #[arg(long)]
    no_k8: bool,
}

impl ShutdownOpt {
    pub async fn process(self) -> Result<(), ClusterCliError> {
        let pb_factory = ProgressBarFactory::new(false);

        let pb = match pb_factory.create() {
            Ok(pb) => pb,
            Err(_) => {
                return Err(ClusterCliError::Other(
                    "Failed to create progress bar".to_string(),
                ))
            }
        };

        if self.local {
            Self::kill_local_processes(&self, &pb).await?;
        } else {
            pb.println(concat!(
                "❌ Shutdown is only implemented for local development.\n",
                "   Please provide '--local' to shutdown the local cluster.",
            ));
        }
        Ok(())
    }

    async fn kill_local_processes(&self, pb: &ProgressRenderer) -> Result<(), ClusterError> {
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

        if !self.no_k8 {
            let _ = self.remove_custom_objects("spus", true);
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
    fn remove_custom_objects(&self, object_type: &str, force: bool) -> Result<(), UninstallError> {
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
