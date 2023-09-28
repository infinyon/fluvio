//! The `show` command is responsible of listing installed Fluvio Versions

use std::path::PathBuf;
use std::str::FromStr;

use color_eyre::eyre::{Error, Result};
use colored::Colorize;
use clap::Parser;
use comfy_table::{Row, Table};

use fluvio_hub_util::fvm::{DEFAULT_PKGSET, Channel};
use fluvio_version_manager::install::{fvm_path, fvm_pkgset_path};
use fluvio_version_manager::package::manifest::{PackageManifest, MANIFEST_FILENAME};
use fluvio_version_manager::settings::Settings;
use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

type PackageVersion = (Channel, PackageManifest);

#[derive(Debug, Parser)]
pub struct ShowOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
}

impl ShowOpt {
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

        if !fvm_pkgset_dir.exists() || fvm_pkgset_dir.read_dir()?.next().is_none() {
            self.notify_fail("No versions installed!");
            self.notify_help(format!(
                "Try running {}, and then retry this command.",
                "fvm install".bold()
            ));
            return Ok(());
        }

        let settings = Settings::open()?;
        let active_channel = settings.active_channel();
        let mut packages = Self::scan_default_pkgset_dir(fvm_pkgset_dir)?;

        packages.sort_by(|(channel_a, _), (channel_b, _)| channel_a.cmp(channel_b));
        Self::print_table(packages, active_channel);

        Ok(())
    }

    /// Walks directories in `~/.fvm/pkgset/default` and collects versions and
    /// metadata from `metadata.json` files
    fn scan_default_pkgset_dir(pkgset_dir: PathBuf) -> Result<Vec<PackageVersion>> {
        let default_pkgset_dir = pkgset_dir.join(DEFAULT_PKGSET);

        let Ok(dir_entries) = default_pkgset_dir.read_dir() else {
            return Err(Error::msg("Failed to read default pkgset dir"));
        };

        let mut packages = Vec::new();

        for entry in dir_entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                let manifest_path = path.join(MANIFEST_FILENAME);
                let manifest = PackageManifest::open(manifest_path)?;
                let dir_name = entry.file_name();
                let channel = Channel::from_str(dir_name.to_str().unwrap())?;

                packages.push((channel, manifest));
            }
        }

        Ok(packages)
    }

    fn print_table(packages: Vec<PackageVersion>, active_channel: Option<Channel>) {
        let mut table = Table::new();

        table.set_header(Row::from(["", "CHANNEL", "VERSION"]));

        for (channel, manifest) in packages {
            if let Some(active_channel) = &active_channel {
                if channel == *active_channel {
                    table.add_row(Row::from([
                        "*",
                        &channel.to_string(),
                        &manifest.version.to_string(),
                    ]));
                    continue;
                }
            }

            table.add_row(Row::from([
                "",
                &channel.to_string(),
                &manifest.version.to_string(),
            ]));
        }

        table.load_preset(comfy_table::presets::NOTHING);
        println!("{}", table);
    }
}

impl Notify for ShowOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
