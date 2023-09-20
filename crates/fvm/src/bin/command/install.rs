//! Initialization Command
//!
//! This command is used to initialize a new Fluvio Version Manager (FVM)
//! instance in the host system.

use color_eyre::eyre::Result;
use clap::Parser;
use tracing::debug;
use url::Url;

use fluvio_hub_util::fvm::RustTarget;

use fluvio_version_manager::common::INFINYON_HUB_URL;
use fluvio_version_manager::install::{InstallTask, Version};
use fluvio_version_manager::setup::{is_fvm_installed, install_fvm};
use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

/// The `init` command is responsible of preparing the workspace for FVM.
#[derive(Debug, Parser)]
pub struct InstallOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
    /// Hosts System Architecture
    #[arg(long)]
    arch: RustTarget,
    /// Package Set to install
    #[arg(long)]
    pkgset: String,
    /// Version to install
    #[arg(long)]
    version: Version,
    /// Registry used to fetch Fluvio Versions
    #[clap(long, default_value = INFINYON_HUB_URL)]
    registry: Url,
}

impl InstallOpt {
    /// Processes install the specified Package Set. If this is the first time
    /// FVM `install` command is being used, or the local FVM isn`t installed,
    /// then it also installs FVM.
    pub async fn process(&self) -> Result<()> {
        if let Some(installed_fvm_path) = is_fvm_installed()? {
            debug!(path=?installed_fvm_path, "FVM is already installed");

            self.install_package().await?;
            return Ok(());
        }

        self.notify_info("Installing FVM...");
        install_fvm()?;

        self.notify_success("FVM installed successfully");
        self.install_package().await?;
        Ok(())
    }

    ///  Performs the installation of the specified `PackageSet`
    async fn install_package(&self) -> Result<()> {
        let install_task = InstallTask::new(
            self.registry.clone(),
            self.pkgset.clone(),
            self.version.clone(),
            self.arch.clone(),
        );
        let _pkgset = install_task.fetch_pkgset().await?;

        Ok(())
    }
}

impl Notify for InstallOpt {
    fn command(&self) -> &'static str {
        "install"
    }

    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
