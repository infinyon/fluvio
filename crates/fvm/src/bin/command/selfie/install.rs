use clap::Parser;
use color_eyre::eyre::Result;
use color_eyre::owo_colors::OwoColorize;

use fluvio_version_manager::install::{fvm_bin_path, install_fvm};
use fluvio_version_manager::settings::Settings;
use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

#[derive(Clone, Debug, Parser)]
pub struct InstallOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
}

impl InstallOpt {
    pub async fn process(&self) -> Result<()> {
        if let Some(installed_fvm_path) = fvm_bin_path()? {
            self.notify_info(format!(
                "FVM is already installed at {}",
                installed_fvm_path.display().italic()
            ));
            return Ok(());
        }

        self.notify_info("Installing FVM...");
        install_fvm()?;
        Settings::create()?;
        self.notify_done("FVM installed successfully");

        Ok(())
    }
}

impl Notify for InstallOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
