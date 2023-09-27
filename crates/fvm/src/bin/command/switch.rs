//! Version Switching Command
//!
//! The `switch` command is responsible of changing the active Fluvio Version

use color_eyre::eyre::Result;
use clap::Parser;
use color_eyre::owo_colors::OwoColorize;

use fluvio_hub_util::fvm::{DEFAULT_PKGSET, Channel};

use fluvio_version_manager::install::{fvm_path, fvm_pkgset_path, fluvio_binaries_path};
use fluvio_version_manager::package::manifest::{MANIFEST_FILENAME, PackageManifest};
use fluvio_version_manager::settings::Settings;
use fluvio_version_manager::switch::overwrite_binaries;
use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

#[derive(Debug, Parser)]
pub struct SwitchOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
    /// Version to install
    #[arg(index = 1, default_value_t = Channel::Stable)]
    version: Channel,
}

impl SwitchOpt {
    pub async fn process(&self) -> Result<()> {
        let fvm_dir = fvm_path()?;
        let fvm_pkgset_dir = fvm_pkgset_path()?;

        if !fvm_dir.exists() {
            self.notify_fail(format!("No {} installation found!", "fvm".bold()));
            self.notify_help(format!(
                "Try running {}, and then retry this command.",
                "fvm self install".bold()
            ));
            return Ok(());
        }

        if !fvm_pkgset_dir.exists() {
            self.notify_fail("No versions installed!");
            self.notify_help(format!(
                "Try running {}, and then retry this command.",
                "fvm install".bold()
            ));
            return Ok(());
        }

        let pkgset_path = fvm_pkgset_dir
            .join(DEFAULT_PKGSET)
            .join(self.version.to_string());

        if !pkgset_path.exists() {
            self.notify_fail(format!(
                "The package {} at version {} is not installed",
                DEFAULT_PKGSET.bold(),
                self.version
            ));

            let help = format!("fvm install {}", self.version);

            self.notify_help(format!(
                "Try running {}, and then retry this command.",
                help.bold()
            ));

            return Ok(());
        }

        self.notify_info(format!(
            "Found package {} with version {}. Setting as default.",
            DEFAULT_PKGSET.bold(),
            self.version.bold()
        ));

        let fluvio_bin_dir = fluvio_binaries_path()?;

        if fluvio_bin_dir.exists() {
            overwrite_binaries(&pkgset_path, &fluvio_bin_dir)?;

            let pkgset_manifest_path = pkgset_path.join(MANIFEST_FILENAME);
            let pkgset_manifest = PackageManifest::open(pkgset_manifest_path)?;
            let mut settings = Settings::open()?;

            settings.set_active(self.version.clone(), pkgset_manifest.version)?;

            self.notify_done(format!(
                "You are now using {} as default {} version",
                self.version.bold(),
                DEFAULT_PKGSET.bold()
            ));

            return Ok(());
        }

        self.notify_warn("No Fluvio workspace found. Install a version first");
        self.notify_help(format!(
            "Try running `fvm install {}` and then retry this command",
            self.version.bold()
        ));

        Ok(())
    }
}

impl Notify for SwitchOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
