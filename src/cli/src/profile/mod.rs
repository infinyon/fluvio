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
pub use sync::CloudError;
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
    /// Display the current context
    #[structopt(name = "current")]
    DisplayCurrent,

    /// Delete the named profile
    #[structopt(name = "delete")]
    Delete(DeleteOpt),

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
pub struct SwitchOpt {
    #[structopt(value_name = "profile name")]
    pub profile_name: String,
}

pub async fn process_profile<O>(
    out: std::sync::Arc<O>,
    profile_command: ProfileCommand,
) -> Result<String, CliError>
where
    O: Terminal,
{
    use context::*;

    match profile_command {
        ProfileCommand::View => view_profile(out),
        ProfileCommand::DisplayCurrent => display_current_profile(out),
        ProfileCommand::Switch(profile) => {
            process_switch(out, profile)?;
        }
        ProfileCommand::Delete(profile) => {
            process_delete(out, profile)?;
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
