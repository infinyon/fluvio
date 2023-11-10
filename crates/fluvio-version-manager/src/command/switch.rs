//! Version Switching Command
//!
//! The `switch` command is responsible of changing the active Fluvio Version

use anyhow::Result;
use clap::Parser;
use colored::Colorize;

use fluvio_hub_util::fvm::Channel;

use crate::common::notify::Notify;
use crate::common::version_directory::VersionDirectory;
use crate::common::workdir::fvm_versions_path;

#[derive(Debug, Parser)]
pub struct SwitchOpt {
    /// Version to set as active
    #[arg(index = 1)]
    version: Option<Channel>,
}

impl SwitchOpt {
    pub async fn process(&self, notify: Notify) -> Result<()> {
        let Some(version) = &self.version else {
            notify.help(format!(
                "You can use {} to see installed versions",
                "fvm show".bold()
            ));

            return Err(anyhow::anyhow!("No version provided"));
        };

        // Ensure the `~/.fvm/versions` directory exists given that we get
        // installed binaries from there. Without this directory we cant
        // switch versions.
        let versions_path = fvm_versions_path()?;

        if !versions_path.exists() {
            notify.warn("No local Fluvio versions found.");
            notify.help(format!(
                "Try installing a version with {}, and then retry this command.",
                "fvm install".bold()
            ));

            return Ok(());
        }

        // Build the path to the version directory requested by the user
        // e.g. Version: 0.10.13 -> ~/.fvm/versions/0.10.13
        let pkgset_path = versions_path.join(version.to_string());

        if !pkgset_path.exists() {
            notify.warn(format!(
                "Fluvio version {} is not installed",
                version.to_string().bold()
            ));

            let help = format!("fvm install {}", version);

            notify.help(format!(
                "Install the desired version using {}, and then retry this command.",
                help.bold()
            ));

            return Ok(());
        }

        let version_dir = VersionDirectory::open(pkgset_path)?;

        version_dir.set_active()?;

        if version.is_version_tag() {
            notify.done(format!(
                "Now using Fluvio version {}",
                version.to_string().bold(),
            ));
        } else {
            notify.done(format!(
                "Now using Fluvio {} ({})",
                version.to_string().bold(),
                version_dir.manifest.version.to_string().bold(),
            ));
        }

        Ok(())
    }
}
