use std::env::current_exe;
use std::fs::{copy, create_dir_all, write};
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use clap::Parser;

use crate::common::executable::remove_fvm_binary_if_exists;
use crate::common::notify::Notify;
use crate::common::settings::Settings;
use crate::common::workdir::{fvm_bin_path, fvm_workdir_path, fvm_versions_path};

const FVM_ENV_FILE_CONTENTS: &str = r#"
#!/bin/sh
case ":${PATH}:" in
    *:"$HOME/.fvm/bin":*)
        ;;
    *)
        export PATH="$PATH:$HOME/.fvm/bin:$HOME/.fluvio/bin"
        ;;
esac
"#;

#[derive(Clone, Debug, Parser)]
pub struct SelfInstallOpt;

impl SelfInstallOpt {
    pub async fn process(&self, notify: Notify) -> Result<()> {
        let fvm_installation_path = self.install_fvm()?;

        Settings::open()?;

        notify.done(format!(
            "FVM installed successfully at {}",
            fvm_installation_path.display()
        ));
        notify.help(format!("Add FVM to PATH using {}", "source $HOME/.fvm/env"));

        Ok(())
    }

    /// Creates the `~/.fvm` directory and copies the current binary to this
    /// directory.
    ///
    /// # Usage of `create_dir` over `create_dir_all`
    ///
    /// Given that on updates the directories may be present, to avoid failing
    /// on `create_dir`, `create_dir_all` is used instead.
    ///
    /// Something similar happens on `mkdir` command, even though underlaying
    /// syscalls may differ.
    ///
    /// Consider the existent directory `~/.fvm/versions`, executing `create_dir`
    /// will fail with error:
    ///
    /// ```ignore
    /// ~/.fvm/versions: File exists
    /// ```
    ///
    /// Instead by doing `create_dir_all` the error will not happen.
    ///
    /// ```ignore
    /// mkdir -p ~/.fvm/versions
    /// ```
    ///
    fn install_fvm(&self) -> Result<PathBuf> {
        // Creates the directory `~/.fvm` if doesn't exists
        let fvm_dir = fvm_workdir_path()?;

        // Creates the binaries directory
        let bin_dir = fvm_dir.join("bin");
        create_dir_all(bin_dir)?;

        let fvm_binary_path = fvm_bin_path()?;
        let current_binary_path = current_exe()?;

        if fvm_binary_path == current_binary_path {
            // We cant replace ourselves, user is running `fvm self install`
            // from the binary itself and not from the installer script.
            return Err(anyhow::anyhow!("FVM is already installed"));
        }

        remove_fvm_binary_if_exists()?;

        // Copies "this" binary to the FVM binary directory
        copy(current_binary_path.clone(), fvm_binary_path.clone()).map_err(|e| {
            anyhow!(
                "Couldn't copy fvm from {} to {} with error {}",
                current_binary_path.display(),
                fvm_binary_path.display(),
                e
            )
        })?;
        tracing::debug!(
            ?fvm_dir,
            "Copied the FVM binary to the FVM home directory with success"
        );

        // Creates the package set directory
        let fvm_pkgset_dir = fvm_versions_path()?;
        create_dir_all(fvm_pkgset_dir)?;

        // Creates the `env` file
        let fvm_env_file_path = fvm_dir.join("env");
        write(fvm_env_file_path, FVM_ENV_FILE_CONTENTS)?;

        Ok(fvm_dir)
    }
}
