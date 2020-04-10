mod cli;

use flv_client::profile::Config;
pub use cli::ProfileCommand;

use crate::Terminal;
use crate::t_println;
use cli::Command;
use crate::error::CliError;

pub(crate) fn process_profile<O>(out: std::sync::Arc<O>,profile_command: ProfileCommand) -> Result<String,CliError>
    where O: Terminal
 {

    let cmd = profile_command.cmd;
    match cmd {
        Command::View =>  view_profile(out.clone()),
        Command::DisplayCurrentProfile => display_current_profile(out.clone()),
        Command::UseProfile(profile) => {
            match Config::load(None) {
                Ok(mut config_file) => {
                    if !config_file.mut_config().set_current_profile(&profile.profile_name) {
                        t_println!(out,"profile {} not founded",config_file.config().current_profile_name());
                    } else {
                        if let Err(err) = config_file.save() {
                            t_println!(out,"unable to save profile: {}",err);
                        }
                    }
                   
                },
                Err(_) => {
                    t_println!(out,"no profile can be founded")
                }
            }
        },
        Command::SetLocal(local) => t_println!(out,"set local")
    }
    
    Ok("".to_owned())
}

fn view_profile<O>(out: std::sync::Arc<O>)
    where O: Terminal
  {

    match Config::load(None) {
        Ok(config_file) => {
            t_println!(out,"{:#?}",config_file.config())
        },
        Err(_) => {
            t_println!(out,"no profile can be founded")
        }
    }
}

fn display_current_profile<O>(out: std::sync::Arc<O>)
    where O: Terminal
  {

    match Config::load(None) {
        Ok(config_file) => {
            t_println!(out,"{}",config_file.config().current_profile_name())
        },
        Err(_) => {
            t_println!(out,"no profile can be founded")
        }
    }
}