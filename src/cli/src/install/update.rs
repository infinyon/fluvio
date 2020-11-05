use structopt::StructOpt;

use crate::CliError;
use fluvio_index::{PackageId, HttpAgent, Target};
use crate::install::{fetch_latest_version, fetch_package_file, install_bin, fluvio_bin_dir};

const FLUVIO_PACKAGE_ID: &str = "fluvio/fluvio";

#[derive(StructOpt, Debug)]
pub struct UpdateOpt {}

impl UpdateOpt {
    pub async fn process(self) -> Result<String, CliError> {
        let agent = HttpAgent::default();
        let output = update_self(&agent).await?;
        Ok(output)
    }
}

async fn update_self(agent: &HttpAgent) -> Result<String, CliError> {
    let target = fluvio_index::PACKAGE_TARGET.parse::<Target>()?;
    let mut id: PackageId = FLUVIO_PACKAGE_ID.parse::<PackageId>()?;

    // Find the latest version of this package
    println!("🎣 Fetching latest version for fluvio/fluvio...");
    let latest_version = fetch_latest_version(agent, &id, target).await?;
    id.version = Some(latest_version);

    // Download the package file from the package registry
    println!("⏳ Downloading Fluvio CLI with latest version: {}...", &id);
    let package_file = fetch_package_file(agent, &id, target).await?;
    println!("🔑 Downloaded and verified package file");

    // Install the package to the ~/.fluvio/bin/ dir
    let fluvio_dir = fluvio_bin_dir()?;
    install_bin(&fluvio_dir, "fluvio", &package_file)?;
    Ok(format!("✅ Successfully installed ~/.fluvio/bin/{}", &id.name))
}

/// Check whether the index requires a more recent version of the client.
///
/// If this is the case, we need to prompt the user to perform an update.
pub async fn check_update_required() -> Result<bool, CliError> {
    let agent = HttpAgent::default();
    let request = agent.request_index()?;
    let response = crate::http::execute(request).await?;
    let index = agent.index_from_response(response).await?;
    Ok(index.metadata.update_required())
}

pub async fn prompt_update() {}
