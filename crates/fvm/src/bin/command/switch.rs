//! Version Switching Command
//!
//! The `switch` command is responsible of changing the active Fluvio Version

use color_eyre::eyre::Result;
use clap::Parser;
use color_eyre::owo_colors::OwoColorize;

use fluvio_hub_util::fvm::{DEFAULT_PKGSET, STABLE_VERSION_CHANNEL};

use fluvio_version_manager::install::{fvm_path, fvm_pkgset_path};
use fluvio_version_manager::switch::{overwrite_binaries, fluvio_bin_path};
use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

#[derive(Debug, Parser)]
pub struct SwitchOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
    /// Version to install
    #[arg(index = 1, default_value = STABLE_VERSION_CHANNEL)]
    version: String,
}

impl SwitchOpt {
    pub async fn process(&self) -> Result<()> {
        let fvm_dir = fvm_path()?;
        let fvm_pkgset_dir = fvm_pkgset_path()?;

        if !fvm_dir.exists() || !fvm_pkgset_dir.exists() {
            self.notify_fail(format!("No {} installation found!", "fvm".bold()));
            self.notify_help(format!(
                "Try running {}, and then retry this command.",
                "fvm install".bold()
            ));
            return Ok(());
        }

        let binaries_dir = fvm_pkgset_dir
            .join(DEFAULT_PKGSET)
            .join(self.version.as_str());

        if !binaries_dir.exists() {
            self.notify_fail(format!(
                "The package {} at version {} is not installed",
                DEFAULT_PKGSET.bold(),
                self.version.as_str()
            ));

            let help = format!(
                "fvm install --pkgset {} --version {}",
                DEFAULT_PKGSET,
                self.version.as_str()
            );

            self.notify_help(format!(
                "Try running {}, and then retry this command.",
                help.bold()
            ));

            return Ok(());
        }

        self.notify_info(format!(
            "Found package {} with version {}. Setting as default.",
            DEFAULT_PKGSET.bold(),
            self.version.as_str().bold()
        ));

        let fluvio_bin = fluvio_bin_path()?;

        overwrite_binaries(&binaries_dir, &fluvio_bin)?;

        self.notify_done(format!(
            "You are now using {} as default {} version",
            self.version.as_str().bold(),
            DEFAULT_PKGSET.bold()
        ));

        Ok(())
    }
}

impl Notify for SwitchOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
