use std::sync::Arc;
use structopt::StructOpt;
use fluvio::config::ConfigFile;
use crate::{Result, Terminal};
use crate::{t_println, t_print_cli_err};

#[derive(Debug, StructOpt)]
pub struct SwitchOpt {
    #[structopt(value_name = "profile name")]
    pub profile_name: String,
}

impl SwitchOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>) -> Result<()> {
        let profile_name = self.profile_name;
        match ConfigFile::load(None) {
            Ok(mut config_file) => {
                if !config_file.mut_config().set_current_profile(&profile_name) {
                    println!("profile {} not found", &profile_name);
                } else if let Err(err) = config_file.save() {
                    println!("unable to save profile: {}", err);
                }
            }
            Err(_) => t_print_cli_err!(out, "no profile can be found"),
        }

        Ok(())
    }
}
