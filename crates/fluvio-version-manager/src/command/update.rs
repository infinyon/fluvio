//! Updates version of the current channel to the most recent one

use anyhow::{Result, Error};
use clap::Args;
use colored::Colorize;
use url::Url;

use fluvio_hub_util::HUB_REMOTE;
use fluvio_hub_util::fvm::{Client, Channel, PackageSet};

use crate::common::version_directory::VersionDirectory;
use crate::common::workdir::fvm_versions_path;
use crate::common::TARGET;
use crate::common::notify::Notify;
use crate::common::settings::Settings;
use crate::common::version_installer::VersionInstaller;

#[derive(Debug, Args)]
pub struct UpdateOpt {
    /// Registry used to fetch Fluvio Versions
    #[arg(long, env = "INFINYON_HUB_REMOTE", default_value = HUB_REMOTE)]
    registry: Url,
}

impl UpdateOpt {
    pub async fn process(self, notify: Notify) -> Result<()> {
        let settings = Settings::open()?;
        let Some(channel) = settings.channel else {
            notify.info("No channel set, please set a channel first using `fvm switch`");
            return Ok(());
        };

        if channel.is_version_tag() {
            // Abort early if the user is not using a Channel and instead has
            // a static tag set as active
            notify.info("Cannot update a static version tag. You must use a channel.");
            return Ok(());
        }

        let latest_pkgset = self.fetch_latest_version(&channel).await?;
        let Some(version) = settings.version else {
            notify.info(
                "No installed version detected, please install a version first using `fvm install`",
            );
            return Ok(());
        };
        let ch_version = Channel::parse(&version)?; // convert to comparable Channel
        let ps_version = Channel::parse(latest_pkgset.pkgset.to_string())?;

        match channel {
            Channel::Stable => {
                if ps_version > ch_version {
                    notify.info(format!(
                        "Updating fluvio {} to version {}. Current version is {}.",
                        channel.to_string().bold(),
                        latest_pkgset.pkgset,
                        version
                    ));

                    return VersionInstaller::new(channel, latest_pkgset, notify)
                        .install()
                        .await;
                }

                if ps_version == ch_version {
                    // Check for patches
                    let curr_version_path = fvm_versions_path()?.join(channel.to_string());
                    let curr_version_dir = VersionDirectory::open(curr_version_path)?;
                    let curr_version_pkgset = curr_version_dir.as_package_set()?;
                    let upstream_artifacts = curr_version_pkgset.artifacts_diff(&latest_pkgset);

                    if !upstream_artifacts.is_empty() {
                        notify.info(format!(
                            "Found {} packages in this version that needs update.",
                            upstream_artifacts.len(),
                        ));

                        return VersionInstaller::new(channel, latest_pkgset, notify)
                            .update(&upstream_artifacts)
                            .await;
                    }
                }

                notify.done("You are already up to date");
            }
            Channel::Latest => {
                // The latest tag can be very dynamic, so we just check for this
                // tag to be different than the current version assuming
                // upstream is always up to date
                if ps_version != ch_version {
                    notify.info(format!(
                        "Updating fluvio {} to version {}. Current version is {}.",
                        channel.to_string().bold(),
                        latest_pkgset.pkgset,
                        version
                    ));

                    return VersionInstaller::new(channel, latest_pkgset, notify)
                        .install()
                        .await;
                }

                notify.done("You are already up to date");
            }
            Channel::Tag(_) | Channel::Other(_) => {
                notify.warn("Static tags cannot be updated. No changes made.");
            }
        }

        Ok(())
    }

    async fn fetch_latest_version(&self, channel: &Channel) -> Result<PackageSet> {
        if channel.is_version_tag() {
            return Err(Error::msg(
                "Cannot update a static version tag. You must use a channel.",
            ));
        }

        let client = Client::new(self.registry.as_str())?;
        let pkgset = client.fetch_package_set(channel, TARGET).await?;

        Ok(pkgset)
    }
}
