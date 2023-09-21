//! Initialization Command
//!
//! This command is used to initialize a new Fluvio Version Manager (FVM)
//! instance in the host system.

use color_eyre::eyre::Result;
use clap::Parser;
use color_eyre::owo_colors::OwoColorize;

use fluvio_version_manager::install::Version;
use fluvio_version_manager::setup::{fvm_path, fvm_pkgset_path};
use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

/// The `init` command is responsible of preparing the workspace for FVM.
#[derive(Debug, Parser)]
pub struct DefaultOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
    /// Package Set to install
    #[arg(long, default_value = "default")]
    pkgset: String,
    /// Version to install
    #[arg(long, default_value = "0.10.14")]
    version: Version,
}

impl DefaultOpt {
    pub async fn process(&self) -> Result<()> {
        let fvm_dir = fvm_path()?;
        let fvm_pkgset_dir = fvm_pkgset_path()?;

        if !fvm_dir.exists() || !fvm_pkgset_dir.exists() {
            self.notify_fail(format!("No {} installation found!", "fvm".bold()));
            self.notify_help(format!("Try running {}, and then retry this command.", "fvm install".bold()));
            return Ok(());
        }

        Ok(())
    }
}

impl Notify for DefaultOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
