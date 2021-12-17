use std::path::{Path, PathBuf};
use structopt::StructOpt;
use tracing::{debug, instrument};

use semver::Version;
use fluvio_index::{PackageId, HttpAgent};
use crate::cli_config::{CliChannelName, FluvioChannelConfig};
use crate::{Result, CliError};
use crate::install::{
    fetch_latest_version, fetch_package_file, install_bin, install_println, fluvio_extensions_dir,
};
use crate::metadata::subcommand_metadata;

const FLUVIO_PACKAGE_ID: &str = "fluvio/fluvio";

#[derive(StructOpt, Debug)]
pub struct UpdateOpt {
    /// Update to the latest prerelease rather than the latest release
    #[structopt(long)]
    develop: bool,

    /// (Optional) the name of one or more plugins to update
    plugins: Vec<PackageId>,
}

impl UpdateOpt {
    pub async fn process(self) -> Result<()> {
        // Verify channel
        // If latest, set the develop flag to true

        let agent = HttpAgent::default();
        let plugin_meta = subcommand_metadata()?;

        // A list of updates to perform. PackageId of the plugin and Path to install
        let mut updates: Vec<(PackageId, PathBuf)> = Vec::new();

        if self.plugins.is_empty() {
            // Collect updates from subcommand metadata
            let plugin_metas: Vec<_> = plugin_meta
                .into_iter()
                .filter(|it| it.meta.package.is_some())
                .collect();

            for plugin in plugin_metas {
                let id = plugin.meta.package.unwrap();
                let path = plugin.path;
                updates.push((id, path));
            }
        } else {
            // Collect updates from the given plugin IDs
            let ext_dir = fluvio_extensions_dir()?;
            for plugin in &self.plugins {
                let path = ext_dir.join(plugin.name().as_str());
                updates.push((plugin.clone(), path));
            }
        }

        self.update_self(&agent).await?;

        if updates.is_empty() {
            println!("👍 No plugins to update, all done!");
            return Ok(());
        }

        let s = if updates.len() != 1 { "s" } else { "" };
        println!(
            "🔧 Preparing update for {} plugin{s}:",
            updates.len(),
            s = s
        );
        for (id, path) in &updates {
            println!("   - {} ({})", id.name(), path.display());
        }

        for (id, path) in &updates {
            self.update_plugin(&agent, id, path).await?;
        }

        Ok(())
    }

    #[instrument(skip(self, agent))]
    async fn update_self(&self, agent: &HttpAgent) -> Result<()> {
        let target = fluvio_index::package_target()?;
        let id: PackageId = FLUVIO_PACKAGE_ID.parse()?;
        debug!(%target, %id, "Fluvio CLI updating self:");

        // Find the latest version of this package
        install_println("🎣 Fetching latest version for fluvio...");
        let latest_version = fetch_latest_version(agent, &id, &target, self.develop).await?;
        let id = id.into_versioned(latest_version.into());

        // Download the package file from the package registry
        install_println(format!(
            "⏳ Downloading Fluvio CLI with latest version: {}...",
            &id.version()
        ));
        let package_result = fetch_package_file(agent, &id, &target).await;
        let package_file = match package_result {
            Ok(pf) => pf,
            Err(CliError::PackageNotFound {
                version, target, ..
            }) => {
                install_println(format!(
                    "❕ Fluvio is not published at version {} for {}, skipping self-update",
                    version, target
                ));
                return Ok(());
            }
            Err(other) => return Err(other),
        };
        install_println("🔑 Downloaded and verified package file");

        // Install the update over the current executable
        let fluvio_path = std::env::current_exe()?;
        install_bin(&fluvio_path, &package_file)?;
        install_println(format!(
            "✅ Successfully updated {}",
            &fluvio_path.display(),
        ));

        Ok(())
    }

    #[instrument(skip(self, agent))]
    async fn update_plugin(&self, agent: &HttpAgent, id: &PackageId, path: &Path) -> Result<()> {
        let target = fluvio_index::package_target()?;
        debug!(%target, %id, "Fluvio CLI updating plugin:");

        let version = fetch_latest_version(agent, id, &target, self.develop).await?;

        println!(
            "⏳ Downloading plugin {} with version {}",
            id.pretty(),
            version
        );
        let id = id.clone().into_versioned(version.into());
        let package_file = fetch_package_file(agent, &id, &target).await?;
        println!("🔑 Downloaded and verified package file");

        println!("✅ Successfully updated {} at ({})", id, path.display());
        install_bin(path, &package_file)?;

        Ok(())
    }
}

/// Check whether the index requires a more recent version of the client.
///
/// If this is the case, we need to prompt the user to perform an update.
#[instrument(
    skip(agent),
    fields(prefix = agent.base_url())
)]
pub async fn check_update_required(agent: &HttpAgent) -> Result<bool> {
    debug!("Checking for a required CLI update");
    let request = agent.request_index()?;
    let response = crate::http::execute(request).await?;
    let index = agent.index_from_response(response).await?;
    Ok(index.metadata.update_required())
}

/// Check whether there is any newer version of the Fluvio CLI available
#[instrument(
    skip(agent),
    fields(prefix = agent.base_url())
)]
pub async fn check_update_available(
    agent: &HttpAgent,
    channel: &FluvioChannelConfig,
) -> Result<Option<Version>> {
    let target = fluvio_index::package_target()?;
    let id: PackageId = FLUVIO_PACKAGE_ID.parse()?;
    debug!(%target, %id, "Checking for an available (not required) CLI update:");

    let prerelease_flag = channel.current_channel() == CliChannelName::Latest;

    let request = agent.request_package(&id)?;
    let response = crate::http::execute(request).await?;
    let package = agent.package_from_response(response).await?;

    let release = package.latest_release_for_target(&target, prerelease_flag)?;
    let latest_version = release.version.clone();
    let current_version =
        Version::parse(&*crate::VERSION).expect("Fluvio CLI 'VERSION' should be a valid semver");

    if current_version < latest_version {
        Ok(Some(latest_version))
    } else {
        Ok(None)
    }
}

/// Prompt the user about a new required version of the Fluvio CLI
#[instrument(
    skip(agent),
    fields(prefix = agent.base_url())
)]
pub async fn prompt_required_update(
    agent: &HttpAgent,
    channel: &FluvioChannelConfig,
) -> Result<()> {
    let target = fluvio_index::package_target()?;
    let id: PackageId = FLUVIO_PACKAGE_ID.parse()?;

    let prerelease_flag = channel.current_channel() == CliChannelName::Latest;

    debug!(%target, %id, "Fetching latest package version:");
    let latest_version = fetch_latest_version(agent, &id, &target, prerelease_flag).await?;

    println!("⚠️ A major update to Fluvio has been detected!");
    println!("⚠️ You must complete this update before using any 'install' command");
    println!(
        "⚠️     Run 'fluvio update' to install v{} of Fluvio",
        &latest_version
    );
    Ok(())
}

/// Prompt the user about a new available version of the Fluvio CLI
pub fn prompt_available_update(latest_version: &Version) {
    println!();
    println!("💡 An update to Fluvio is available!");
    println!(
        "💡     Run 'fluvio update' to install v{} of Fluvio",
        &latest_version
    );
}
