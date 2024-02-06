//! Show Intalled Versions Command
//!
//! The `show` command is responsible of listing all the installed Fluvio Versions

use anyhow::Result;
use clap::Parser;
use colored::Colorize;

use fluvio_hub_util::fvm::Channel;

use crate::common::notify::Notify;
use crate::common::settings::Settings;

#[derive(Debug, Parser)]
pub struct CurrentOpt;

impl CurrentOpt {
    pub async fn process(&self, notify: Notify) -> Result<()> {
        let settings = Settings::open()?;

        if let (Some(channel), Some(version)) = (settings.channel, settings.version) {
            match channel {
                Channel::Latest | Channel::Stable => println!("{} ({})", version, channel),
                _ => println!("{}", version),
            }
        } else {
            notify.warn("No active version set");
            notify.help(format!(
                "You can use {} to set the active version",
                "fvm switch".bold()
            ));
        }

        Ok(())
    }
}
