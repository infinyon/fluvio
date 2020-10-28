use std::io::{ErrorKind, Error as IoError};
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::debug;

use crate::CliError;
use fluvio_index::{PackageId, HttpAgent, Target};
use thiserror::private::DisplayAsDisplay;

const FLUVIO_PACKAGE_ID: &str = "fluvio/fluvio";

fn fluvio_bin_dir() -> Result<PathBuf, CliError> {
    let home = dirs::home_dir()
        .ok_or_else(|| IoError::new(ErrorKind::NotFound, "Homedir not found"))?;
    Ok(home.join(".fluvio/bin/"))
}

#[derive(StructOpt, Debug)]
pub struct UpdateOpt { }

impl UpdateOpt {
    pub async fn process(self) -> Result<String, CliError> {
        let agent = HttpAgent::default();
        update_self(&agent).await?;

        Ok("".to_string())
    }
}

async fn update_self(agent: &HttpAgent) -> Result<String, CliError> {
    let target = fluvio_index::PACKAGE_TARGET.parse::<Target>()?;
    let mut id: PackageId = FLUVIO_PACKAGE_ID.parse::<PackageId>()?;

    let request = agent.request_package(&id)?;
    debug!("Requesting package manifest: {}", request.url().as_display());
    let response = crate::http::execute(request).await?;
    let package = agent.package_from_response(response).await?;
    let latest_release = package.latest_release_for_target(target)?;
    debug!(release = ?latest_release, "Latest release for package:");
    if !latest_release.target_exists(target) {
        return Err(fluvio_index::Error::MissingTarget(target).into());
    }

    // Download the package file from the package registry
    id.version = Some(latest_release.version.clone());
    let download_request = agent.request_release_download(&id, target)?;
    debug!(url = %download_request.url(), "Requesting package download:");
    let response = crate::http::execute(download_request).await?;
    let package_file = agent.release_from_response(response).await?;

    // Download the package checksum from the package registry
    let checksum_request = agent.request_release_checksum(&id, target)?;
    let response = crate::http::execute(checksum_request).await?;
    let package_checksum = agent.checksum_from_response(response).await?;

    if !verify_checksum(&package_file, &package_checksum) {
        return Err(fluvio_index::Error::ChecksumError.into());
    }
    debug!(hex = %package_checksum, "Verified checksum");

    // Create ~/.fluvio/bin/ if it does not exist
    let download_dir = fluvio_bin_dir()?;
    std::fs::create_dir_all(&download_dir)?;

    // Download our package to `~/.fluvio/bin/<name>`
    let download_path = download_dir.join(id.name.as_str());
    std::fs::write(&download_path, &package_file)?;

    // Mark our package as executable
    let status = std::process::Command::new("chmod")
        .arg("+x")
        .arg(download_path.as_os_str())
        .status()?;
    if !status.success() {
        return Err(CliError::Other("Failed to make package executable".to_string()));
    }

    Ok(format!("Successfully installed ~/.fluvio/bin/{}", &id.name))
}

fn verify_checksum<B: AsRef<[u8]>>(buffer: B, checksum: &str) -> bool {
    let bytes = buffer.as_ref();
    let buffer_checksum = {
        use sha2::Digest as _;
        let mut hasher = sha2::Sha256::new();
        hasher.update(bytes);
        let output = hasher.finalize();
        hex::encode(output)
    };
    &*buffer_checksum == checksum
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

pub async fn prompt_update() {

}
