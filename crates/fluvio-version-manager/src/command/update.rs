//! Updates version of the current channel to the most recent one

use anyhow::{Result, Error};
use clap::Args;
use colored::Colorize;
use url::Url;

use fluvio_hub_util::HUB_REMOTE;
use fluvio_hub_util::fvm::{Client, Channel, PackageSet};
use fluvio_version::build::TARGET;

use crate::common::notify::Notify;
use crate::common::settings::Settings;
use crate::common::version_installer::VersionInstaller;

#[derive(Debug, Args)]
pub struct UpdateOpt {
    /// Registry used to fetch Fluvio Versions
    #[arg(long, env = "HUB_REGISTRY_URL", default_value = HUB_REMOTE)]
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

        match channel {
            Channel::Stable => {
                if latest_pkgset.pkgset > version {
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
            Channel::Latest => {
                // The latest tag can be very dynamic, so we just check for this
                // tag to be different than the current version assuming
                // upstream is always up to date
                if latest_pkgset.pkgset != version {
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
            Channel::Tag(_) => {
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
