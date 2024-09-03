use std::ffi::OsString;
use std::fs::{remove_dir_all, remove_file};
use std::path::Path;

use fluvio_types::defaults::SPU_MONITORING_UNIX_SOCKET;
use sysinfo::System;
use anyhow::Result;

use tracing::{debug, warn};

use crate::render::ProgressRenderer;
use crate::start::local::{DEFAULT_DATA_DIR, LOCAL_CONFIG_PATH};

pub async fn kill_local_processes(pb: &ProgressRenderer) -> Result<()> {
    pb.set_message("Uninstalling fluvio local components");

    let kill_proc = |name: &str, command_args: Option<&[String]>| {
        sysinfo::set_open_files_limit(0);
        let mut sys = System::new();
        sys.refresh_processes(sysinfo::ProcessesToUpdate::All); // Only load what we need.
        for process in sys.processes_by_exact_name(name.as_ref()) {
            if let Some(cmd_args) = command_args {
                let proc_cmds = process.cmd();
                if cmd_args.len() > proc_cmds.len() {
                    continue; // Ignore procs with less command_args than the target.
                }
                if cmd_args
                    .iter()
                    .map(OsString::from)
                    .collect::<Vec<_>>()
                    .iter()
                    .ne(proc_cmds[..cmd_args.len()].iter())
                {
                    continue; // Ignore procs which don't match.
                }
            }
            if !process.kill() {
                // This will fail if called on a proc running as root, so only log failure.
                debug!(
                    "Sysinto process.kill() returned false. pid: {}, name: {}: user: {:?}",
                    process.pid(),
                    process.name().to_str().unwrap_or("unknown"),
                    process.user_id(),
                );
            }
        }
    };
    kill_proc("fluvio", Some(&["cluster".into(), "run".into()]));
    kill_proc("fluvio", Some(&["run".into()]));
    kill_proc("fluvio-run", None);

    Ok(())
}

pub fn delete_fs<T: AsRef<Path>>(
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
                Err(io_err) if io_err.kind() == std::io::ErrorKind::NotFound => {
                    debug!("{} not found: {}", tag, path_ref.display());
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

pub fn delete_spu_socket() {
    delete_fs(
        Some(SPU_MONITORING_UNIX_SOCKET),
        "SPU monitoring socket",
        true,
        None,
    );
}

pub fn delete_local_config() {
    delete_fs(
        LOCAL_CONFIG_PATH.as_ref(),
        "local cluster config",
        true,
        None,
    );
}

pub fn delete_data_dir() {
    delete_fs(DEFAULT_DATA_DIR.as_ref(), "data dir", false, None);
}
