use std::fs::{remove_file};
use std::process::Command;

use clap::Parser;
use colored::Colorize;
use fluvio::config::ConfigFile;
use fluvio_command::CommandExt;
use tracing::{info, debug};
use sysinfo::{ProcessExt, System, SystemExt};

use fluvio_types::defaults::SPU_MONITORING_UNIX_SOCKET;

use crate::render::ProgressRenderer;
use crate::{cli::ClusterCliError};
use crate::progress::ProgressBarFactory;
use crate::ClusterError;
use crate::error::UninstallError;
use crate::{DEFAULT_NAMESPACE};

#[derive(Debug, Parser)]
pub struct ShutdownOpt {}

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

        let config_file = ConfigFile::load_default_or_new()?;

        let profile_name = Self::profile_name(&config_file);
        if profile_name != "local".to_string() {
            pb.println(format!(
                "{} Profile {} found, shutdown is only implemented for profile \"local\"",
                "❌",
                profile_name.italic(),
            ));
        }

        Self::kill_local_processes(&self, &pb).await?;

        // Notify the k8s cluster we have removed the spus so that
        // `fluvio cluster start --local --develop` can be used to restart the cluster
        let _ = self.remove_custom_objects(
            "spus",
            &DEFAULT_NAMESPACE.to_string(),
            None,
            false,
            &pb
        );

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

        // remove monitoring socket
        match remove_file(SPU_MONITORING_UNIX_SOCKET) {
            Ok(_) => {
                pb.println(format!(
                    "Removed spu monitoring socket: {}",
                    SPU_MONITORING_UNIX_SOCKET
                ));
            }
            Err(err) => {
                pb.println(format!(
                    "SPU monitoring socket  {}, can't be removed: {}",
                    SPU_MONITORING_UNIX_SOCKET, err
                ));
            }
        }

        pb.println("Uninstalled fluvio local components");
        pb.finish_and_clear();

        Ok(())
    }

    fn profile_name(config_file: &ConfigFile) -> String {
        config_file
            .config()
            .current_profile_name()
            .unwrap()
            .to_string()
    }

    /// Remove objects of specified type, namespace
    fn remove_custom_objects(
        &self,
        object_type: &str,
        namespace: &str,
        selector: Option<&str>,
        force: bool,
        pb: &ProgressRenderer,
    ) -> Result<(), UninstallError> {
        pb.set_message(format!("Removing {} objects", object_type));
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
}
