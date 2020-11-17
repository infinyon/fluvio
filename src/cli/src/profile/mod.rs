//!
//! # Produce CLI
//!
//! CLI command for Profile operation
//!

mod sync;
mod k8;
mod context;

use sync::SyncCommand;
use structopt::StructOpt;
pub use k8::set_k8_context;
pub use k8::discover_fluvio_addr;

pub use context::set_local_context;
pub use sync::LocalOpt;
pub use sync::K8Opt;

use fluvio::config::ConfigFile;

use crate::Terminal;
use crate::error::CliError;
use crate::t_println;
use crate::t_print_cli_err;
use crate::profile::sync::process_sync;

#[derive(Debug, StructOpt)]
#[structopt(about = "Available Commands")]
pub enum ProfileCommand {
    /// Print the name of the current context
    #[structopt(name = "current")]
    DisplayCurrent,

    /// Delete the named profile
    #[structopt(name = "delete")]
    Delete(DeleteOpt),

    /// Delete the named cluster
    #[structopt(name = "delete-cluster")]
    DeleteCluster(DeleteClusterOpt),

    /// Switch to the named profile
    #[structopt(name = "switch")]
    Switch(SwitchOpt),

    /// Sync a profile from a cluster
    #[structopt(name = "sync")]
    Sync(SyncCommand),

    /// Display entire configuration
    #[structopt(name = "view")]
    View,
}

#[derive(Debug, StructOpt)]
pub struct DeleteOpt {
    #[structopt(value_name = "profile name")]
    pub profile_name: String,
}

#[derive(Debug, StructOpt)]
pub struct DeleteClusterOpt {
    /// The name of a cluster connection to delete
    #[structopt(value_name = "cluster name")]
    pub cluster_name: String,
    /// Deletes a cluster even if its active
    #[structopt(short, long)]
    pub force: bool,
}

#[derive(Debug, StructOpt)]
pub struct SwitchOpt {
    #[structopt(value_name = "profile name")]
    pub profile_name: String,
}

pub async fn process_profile<O>(
    out: std::sync::Arc<O>,
    profile_command: ProfileCommand,
) -> eyre::Result<String>
where
    O: Terminal,
{
    use context::*;

    match profile_command {
        ProfileCommand::View => view_profile(out)?,
        ProfileCommand::DisplayCurrent => display_current_profile(out),
        ProfileCommand::Switch(profile) => {
            process_switch(out, profile)?;
        }
        ProfileCommand::Delete(profile) => {
            process_delete(out, profile)?;
        }
        ProfileCommand::DeleteCluster(cluster) => {
            process_delete_cluster(out, cluster)?;
        }
        ProfileCommand::Sync(profile) => {
            process_sync(out, profile).await?;
        }
    }

    Ok("".to_owned())
}

pub fn process_switch<O>(out: std::sync::Arc<O>, opt: SwitchOpt) -> Result<String, CliError>
where
    O: Terminal,
{
    let profile_name = opt.profile_name;
    match ConfigFile::load(None) {
        Ok(mut config_file) => {
            if !config_file.mut_config().set_current_profile(&profile_name) {
                t_println!(out, "profile {} not found", &profile_name);
            } else if let Err(err) = config_file.save() {
                t_println!(out, "unable to save profile: {}", err);
            }
        }
        Err(_) => t_print_cli_err!(out, "no profile can be found"),
    }

    Ok("".to_string())
}

pub fn process_delete<O>(out: std::sync::Arc<O>, opt: DeleteOpt) -> Result<String, CliError>
where
    O: Terminal,
{
    let profile_name = opt.profile_name;
    match ConfigFile::load(None) {
        Ok(mut config_file) => {
            if !config_file.mut_config().delete_profile(&profile_name) {
                t_println!(out, "profile {} not found", &profile_name);
            } else if let Err(err) = config_file.save() {
                t_println!(out, "unable to save profile: {}", err);
            } else {
                t_println!(out, "profile {} deleted", &profile_name);
                if config_file.config().current_profile_name().is_none() {
                    t_println!(out,"warning: this removed your current profile, use 'config switch-profile to select a different one");
                } else {
                    t_println!(out, "profile deleted");
                }
            }
        }
        Err(_) => t_print_cli_err!(out, "no profile can be found"),
    }

    Ok("".to_string())
}

pub fn process_delete_cluster<O>(
    _out: std::sync::Arc<O>,
    opt: DeleteClusterOpt,
) -> Result<String, CliError>
where
    O: Terminal,
{
    let cluster_name = opt.cluster_name;

    let mut config_file = match ConfigFile::load(None) {
        Ok(config_file) => config_file,
        Err(e) => {
            println!("No config can be found: {}", e);
            return Ok("".to_string());
        }
    };

    let config = config_file.mut_config();

    // Check if the named cluster exists
    if config.cluster(&cluster_name).is_none() {
        println!("No profile named {} exists", &cluster_name);
        return Ok("".to_string());
    }

    if !opt.force {
        // Check whether there are any profiles that conflict with
        // this cluster being deleted. That is, if any profiles reference it.
        if let Err(profile_conflicts) = config.delete_cluster_check(&cluster_name) {
            println!(
                "The following profiles reference cluster {}:",
                &cluster_name
            );
            for profile in profile_conflicts.iter() {
                println!("  {}", profile);
            }
            println!("If you would still like to delete the cluster, use --force");
            return Ok("".to_string());
        }
    }

    let _deleted = match config.delete_cluster(&cluster_name) {
        Some(deleted) => deleted,
        None => {
            println!("Cluster {} not found", &cluster_name);
            return Ok("".to_string());
        }
    };

    match config_file.save() {
        Ok(_) => Ok(format!("Cluster {} deleted", &cluster_name)),
        Err(e) => {
            println!("Unable to save config file: {}", e);
            Ok("".to_string())
        }
    }
}
