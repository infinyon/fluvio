use std::sync::Arc;

use clap::Parser;
use anyhow::Result;

use fluvio::config::ConfigFile;

use crate::common::output::Terminal;
use crate::common::{t_println, t_print_cli_err};

#[derive(Debug, Parser)]
pub struct DeleteProfileOpt {
    #[clap(value_name = "profile name")]
    pub profile_name: String,
}

impl DeleteProfileOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>) -> Result<()> {
        let profile_name = self.profile_name;
        match ConfigFile::load(None) {
            Ok(mut config_file) => {
                if !config_file.mut_config().delete_profile(&profile_name) {
                    println!("profile {} not found", &profile_name);
                } else if let Err(err) = config_file.save() {
                    println!("unable to save profile: {err}");
                } else {
                    println!("profile {} deleted", &profile_name);
                    if config_file.config().current_profile_name().is_none() {
                        println!("warning: this removed your current profile, use 'fluvio profile switch' to select a different one");
                    } else {
                        println!("profile deleted");
                    }
                }
            }
            Err(_) => t_print_cli_err!(out, "no profile can be found"),
        }

        Ok(())
    }
}
