use std::env::current_exe;
use std::fs::{create_dir, copy};
use std::path::PathBuf;

use clap::Parser;
use color_eyre::eyre::Result;
use color_eyre::owo_colors::OwoColorize;

use crate::GlobalOptions;

use crate::common::notify::Notify;
use crate::common::settings::Settings;
use crate::common::workdir::{fvm_bin_path, fvm_workdir_path, fvm_pkgset_path};

#[derive(Clone, Debug, Parser)]
pub struct InstallOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
}

impl InstallOpt {
    pub async fn process(&self) -> Result<()> {
        // Checks if FVM is already installed
        let bin_path = fvm_bin_path()?;

        if bin_path.exists() {
            self.notify_info(format!(
                "FVM is already installed at {}",
                bin_path.display().italic()
            ));
            return Ok(());
        }

        let fvm_installation_path = self.install_fvm()?;
        Settings::init()?;

        self.notify_done(format!(
            "FVM installed successfully at {}",
            fvm_installation_path.display().italic()
        ));

        Ok(())
    }

    /// Creates the `~/.fvm` directory and copies the current binary to this
    /// directory.
    fn install_fvm(&self) -> Result<PathBuf> {
        // Creates the directory `~/.fvm` if doesn't exists
        let fvm_dir = fvm_workdir_path()?;

        if !fvm_dir.exists() {
            create_dir(&fvm_dir)?;
            tracing::debug!(?fvm_dir, "Created FVM home directory with success");
        }

        // Attempts to create the binary crate
        let fvm_binary_dir = fvm_dir.join("bin");
        create_dir(&fvm_binary_dir)?;
        tracing::debug!(?fvm_binary_dir, "Created FVM bin directory with success");

        // Copies "this" binary to the FVM binary directory
        let current_binary_path = current_exe()?;
        let fvm_binary_path = fvm_bin_path()?;

        copy(current_binary_path, fvm_binary_path)?;
        tracing::debug!(
            ?fvm_dir,
            "Copied the FVM binary to the FVM home directory with success"
        );

        // Creates the package set directory
        let fvm_pkgset_dir = fvm_pkgset_path()?;
        create_dir(fvm_pkgset_dir)?;

        Ok(fvm_dir)
    }
}

impl Notify for InstallOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
