use structopt::StructOpt;
use tracing::{debug, instrument};

use semver::Version;
use fluvio_index::{PackageId, HttpAgent, MaybeVersion};
use crate::CliError;
use crate::install::{fetch_latest_version, fetch_package_file, install_bin, install_println};
use crate::metadata::{SubcommandMetadata, subcommand_metadata};

const FLUVIO_PACKAGE_ID: &str = "fluvio/fluvio";

#[derive(StructOpt, Debug)]
pub struct UpdateOpt {
    /// Update to the latest prerelease rather than the latest release
    #[structopt(long)]
    develop: bool,

    /// Update all plugins
    #[structopt(long)]
    all: bool,

    /// (Optional) the name of one or more plugins to update
    plugins: Vec<PackageId<MaybeVersion>>,
}

impl UpdateOpt {
    pub async fn process(self) -> Result<(), CliError> {
        let agent = HttpAgent::default();

        if self.plugins.is_empty() {
            self.update_self(&agent).await?;
            return Ok(());
        }

        let plugin_metas: Vec<_> = subcommand_metadata()?
            .into_iter()
            .filter(|it| it.meta.package.is_some())
            .collect();

        for plugin in &self.plugins {
            let plugin_meta = plugin_metas
                .iter()
                .find(|it| it.meta.package.as_ref().unwrap().uid() == plugin.uid())
                .ok_or_else(|| CliError::Other("Unable to find plugin".to_string()))?;

            self.update_plugin(&agent, plugin_meta).await?;
        }

        Ok(())
    }

    #[instrument(skip(self, agent))]
    async fn update_self(&self, agent: &HttpAgent) -> Result<(), CliError> {
        let target = fluvio_index::package_target()?;
        let id: PackageId<MaybeVersion> = FLUVIO_PACKAGE_ID.parse()?;
        debug!(%target, %id, "Fluvio CLI updating self:");

        // Find the latest version of this package
        install_println("🎣 Fetching latest version for fluvio/fluvio...");
        let latest_version = fetch_latest_version(agent, &id, target, self.develop).await?;
        let id = id.into_versioned(latest_version);

        // Download the package file from the package registry
        install_println(format!(
            "⏳ Downloading Fluvio CLI with latest version: {}...",
            &id
        ));
        let package_file = fetch_package_file(agent, &id, target).await?;
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

    #[instrument(skip(self, agent, plugin))]
    async fn update_plugin(
        &self,
        agent: &HttpAgent,
        plugin: &SubcommandMetadata,
    ) -> Result<(), CliError> {
        let target = fluvio_index::package_target()?;
        let id = plugin
            .meta
            .package
            .as_ref()
            .ok_or_else(|| CliError::Other("Plugin does not specify package".to_string()))?;
        debug!(%target, %id, "Fluvio CLI updating plugin:");

        let version = fetch_latest_version(agent, id, target, self.develop).await?;

        println!("Found latest version: {}", version);
        let id = id.clone().into_versioned(version);
        let package_file = fetch_package_file(agent, &id, target).await?;

        println!("Installing plugin at path {}", plugin.path.display());
        install_bin(&plugin.path, &package_file)?;

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
pub async fn check_update_required(agent: &HttpAgent) -> Result<bool, CliError> {
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
    prerelease: bool,
) -> Result<Option<Version>, CliError> {
    let target = fluvio_index::package_target()?;
    let id: PackageId<MaybeVersion> = FLUVIO_PACKAGE_ID.parse()?;
    debug!(%target, %id, "Checking for an available (not required) CLI update:");

    let request = agent.request_package(&id)?;
    let response = crate::http::execute(request).await?;
    let package = agent.package_from_response(response).await?;

    let release = package.latest_release_for_target(target, prerelease)?;
    let latest_version = release.version.clone();
    let current_version =
        Version::parse(crate::VERSION).expect("Fluvio CLI 'VERSION' should be a valid semver");

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
pub async fn prompt_required_update(agent: &HttpAgent) -> Result<(), CliError> {
    let target = fluvio_index::package_target()?;
    let id: PackageId<MaybeVersion> = FLUVIO_PACKAGE_ID.parse()?;
    debug!(%target, %id, "Fetching latest package version:");
    let latest_version = fetch_latest_version(agent, &id, target, false).await?;

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
