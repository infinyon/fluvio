//!
//! # Produce CLI
//!
//! CLI command for Profile operation
//!

use crate::tls::TlsConfig;
use structopt::StructOpt;

use flv_client::config::ConfigFile;

use crate::Terminal;
use crate::t_println;
use crate::t_print_cli_err;
use crate::error::CliError;

#[derive(Debug, StructOpt, Default)]
pub struct InlineProfile {
    #[structopt(short = "P", long, value_name = "profile")]
    pub profile: Option<String>,
}

#[derive(Debug, StructOpt)]
#[structopt(about = "Available Commands")]
pub enum Command {
    /// Display the current context
    #[structopt(name = "current-profile")]
    DisplayCurrentProfile,

    #[structopt(name = "switch-profile")]
    UseProfile(UseProfile),

    #[structopt(name = "delete-profile")]
    DeleteProfile(DeleteProfile),

    /// set profile to local servers
    #[structopt(name = "create-local-profile")]
    SetLocalProfile(SetLocal),

    /// set profile to kubernetes cluster
    #[structopt(name = "create-k8-profile")]
    SetK8Profile(SetK8),

    /// Display entire configuration
    #[structopt(name = "view")]
    View,
}

#[derive(Debug, StructOpt)]
pub struct ProfileCommand {
    /// set local context with new sc address
    //  #[structopt(short,long, value_name = "host:port")]
    //  pub local: Option<String>,

    #[structopt(subcommand)]
    pub cmd: Command,
}

#[derive(Debug, Default, StructOpt)]
pub struct SetLocal {
    #[structopt(value_name = "host:port", default_value = "localhost:9003")]
    pub local: String,

    #[structopt(flatten)]
    pub tls: TlsConfig,
}

#[derive(Debug, StructOpt, Default)]
pub struct SetK8 {
    /// kubernetes namespace,
    #[structopt(long, short, value_name = "namespace")]
    pub namespace: Option<String>,

    /// profile name
    #[structopt(value_name = "name")]
    pub name: Option<String>,

    #[structopt(flatten)]
    pub tls: TlsConfig,
}

#[derive(Debug, StructOpt)]
pub struct UseProfile {
    #[structopt(value_name = "profile name")]
    pub profile_name: String,
}

#[derive(Debug, StructOpt)]
pub struct DeleteProfile {
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
    use super::context::*;
    use super::k8::*;

    let cmd = profile_command.cmd;
    match cmd {
        Command::View => view_profile(out.clone()),
        Command::DisplayCurrentProfile => display_current_profile(out.clone()),
        Command::UseProfile(profile) => match ConfigFile::load(None) {
            Ok(mut config_file) => {
                if !config_file
                    .mut_config()
                    .set_current_profile(&profile.profile_name)
                {
                    t_println!(out, "profile {} not founded", profile.profile_name);
                } else {
                    if let Err(err) = config_file.save() {
                        t_println!(out, "unable to save profile: {}", err);
                    }
                }
            }
            Err(_) => t_print_cli_err!(out, "no profile can be founded"),
        },
        Command::SetLocalProfile(opt) => match set_local_context(opt) {
            Ok(msg) => t_println!(out, "{}", msg),
            Err(err) => {
                eprintln!("config creation failed: {}", err);
            }
        },
        Command::SetK8Profile(opt) => match set_k8_context(opt).await {
            Ok(msg) => t_println!(out, "{}", msg),
            Err(err) => {
                eprintln!("config creation failed: {}", err);
            }
        },
        Command::DeleteProfile(profile) => match ConfigFile::load(None) {
            Ok(mut config_file) => {
                if !config_file
                    .mut_config()
                    .delete_profile(&profile.profile_name)
                {
                    t_println!(out, "profile {} not founded", profile.profile_name);
                } else {
                    if let Err(err) = config_file.save() {
                        t_println!(out, "unable to save profile: {}", err);
                    } else {
                        t_println!(out, "profile {} deleted", profile.profile_name);
                        if config_file.config().current_profile_name().is_none() {
                            t_println!(out,"warning: this removed your current profile, use 'config switch-profile to select a different one");
                        } else {
                            t_println!(out, "profile deleted");
                        }
                    }
                }
            }
            Err(_) => t_print_cli_err!(out, "no profile can be founded"),
        },
    }

    Ok("".to_owned())
}
