//! Install Command
//!
//! Downloads and stores the sepecific Fluvio Version binaries in the local
//! FVM cache.

use anyhow::Result;
use clap::Parser;

use colored::Colorize;
use fluvio_hub_util::fvm::Channel;

use crate::common::notify::Notify;

use crate::common::version_directory::VersionDirectory;

use crate::common::workdir::fvm_versions_path;

/// The `install` command is responsible of installing the desired Package Set
#[derive(Debug, Parser)]
pub struct UninstallOpt {
    /// Version to install: stable, latest, or named-version x.y.z
    #[arg(index = 1, default_value_t = Channel::Stable)]
    version: Channel,
}

impl UninstallOpt {
    pub async fn process(&self, notify: Notify) -> Result<()> {
        let versions_path = fvm_versions_path()?;

        if !versions_path.exists() {
            notify.warn("No versions installed");
            return Ok(());
        }

        let pkgset_path = versions_path.join(self.version.to_string());

        if !pkgset_path.exists() {
            notify.warn(format!(
                "Fluvio version {} is not installed",
                self.version.to_string().bold()
            ));

            return Ok(());
        }

        let version_directory = VersionDirectory::open(pkgset_path)?;
        version_directory.remove()?;

        Ok(())
    }
}
