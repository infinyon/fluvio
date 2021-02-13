use structopt::StructOpt;
use tracing::{debug, instrument};

use semver::Version;
use fluvio_index::{PackageId, HttpAgent, MaybeVersion};
use crate::CliError;
use crate::install::{fetch_latest_version, fetch_package_file, install_bin, install_println};

const FLUVIO_PACKAGE_ID: &str = "fluvio/fluvio";

#[derive(StructOpt, Debug)]
pub struct UpdateOpt {
    /// Used for testing. Specifies alternate package location, e.g. "test/"
    #[structopt(hidden = true, long)]
    prefix: Option<String>,
    /// Update to the latest prerelease rather than the latest release
    #[structopt(long)]
    develop: bool,
}

impl UpdateOpt {
    pub async fn process(self) -> Result<String, CliError> {
        let agent = match &self.prefix {
            Some(prefix) => HttpAgent::with_prefix(prefix)?,
            None => HttpAgent::default(),
        };
        let output = self.update_self(&agent).await?;
        Ok(output)
    }

    #[instrument(skip(agent))]
    async fn update_self(&self, agent: &HttpAgent) -> Result<String, CliError> {
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

        Ok("".to_string())
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
pub async fn check_update_available(agent: &HttpAgent, prerelease: bool) -> Result<bool, CliError> {
    let target = fluvio_index::package_target()?;
    let id: PackageId<MaybeVersion> = FLUVIO_PACKAGE_ID.parse()?;
    debug!(%target, %id, "Checking for an available (not required) CLI update:");

    let request = agent.request_package(&id)?;
    let response = crate::http::execute(request).await?;
    let package = agent.package_from_response(response).await?;

    let release = package.latest_release_for_target(target, prerelease)?;
    let latest_version = &release.version;
    let current_version =
        Version::parse(crate::VERSION).expect("Fluvio CLI 'VERSION' should be a valid semver");

    Ok(current_version < *latest_version)
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
#[instrument(
    skip(agent),
    fields(prefix = agent.base_url())
)]
pub async fn prompt_available_update(agent: &HttpAgent, prerelease: bool) -> Result<(), CliError> {
    let target = fluvio_index::package_target()?;
    let id: PackageId<MaybeVersion> = FLUVIO_PACKAGE_ID.parse()?;
    debug!(%target, %id, "Fetching latest package version:");
    let latest_version = fetch_latest_version(agent, &id, target, prerelease).await?;

    println!();
    println!("💡 An update to Fluvio is available!");
    println!(
        "💡     Run 'fluvio update' to install v{} of Fluvio",
        &latest_version
    );
    Ok(())
}
