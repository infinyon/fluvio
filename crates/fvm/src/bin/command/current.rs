//! The `current` command is responsible of printing the active Fluvio Version

use color_eyre::eyre::Result;
use colored::Colorize;
use clap::Parser;

use fluvio_hub_util::fvm::Channel;

use fluvio_version_manager::install::fvm_bin_path;
use fluvio_version_manager::settings::Settings;
use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

#[derive(Debug, Parser)]
pub struct CurrentOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
}

impl CurrentOpt {
    pub async fn process(&self) -> Result<()> {
        if fvm_bin_path()?.is_some() {
            let settings = Settings::open()?;

            if let (Some(channel), Some(version)) = settings.version_parts() {
                match channel {
                    Channel::Stable => println!("{} ({})", version, "stable".green()),
                    Channel::Latest => println!("{} ({})", version, "latest".purple()),
                    Channel::Tag(_) => println!("{}", version),
                }

                return Ok(());
            }

            self.notify_warn("No active Fluvio version found");
            self.notify_help(format!(
                "Try running {}",
                "fvm switch <VERSION | CHANNEL>".bold()
            ));

            return Ok(());
        }

        self.notify_warn("No FVM installation found");
        self.notify_help(format!(
            "Try running {} and then retry this command",
            "fvm self install".bold()
        ));

        Ok(())
    }
}

impl Notify for CurrentOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
