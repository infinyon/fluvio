use std::io::{ErrorKind, Error as IoError};
use std::path::{Path, PathBuf};
use tracing::debug;
use semver::Version;
use fluvio_index::{HttpAgent, PackageId, Target};
use crate::CliError;

pub mod update;
pub mod plugins;

fn fluvio_bin_dir() -> Result<PathBuf, CliError> {
    let home =
        dirs::home_dir().ok_or_else(|| IoError::new(ErrorKind::NotFound, "Homedir not found"))?;
    Ok(home.join(".fluvio/bin/"))
}

/// Fetches the latest version of the package with the given ID
async fn fetch_latest_version(
    agent: &HttpAgent,
    id: &PackageId,
    target: Target,
) -> Result<Version, CliError> {
    let request = agent.request_package(id)?;
    debug!(
        url = %request.url(),
        "Requesting package manifest:",
    );
    let response = crate::http::execute(request).await?;
    let package = agent.package_from_response(response).await?;
    let latest_release = package.latest_release_for_target(target)?;
    debug!(release = ?latest_release, "Latest release for package:");
    if !latest_release.target_exists(target) {
        return Err(fluvio_index::Error::MissingTarget(target).into());
    }
    Ok(latest_release.version.clone())
}

/// Downloads and verifies a package file via it's versioned ID and target
async fn fetch_package_file(
    agent: &HttpAgent,
    id: &PackageId,
    target: Target,
) -> Result<Vec<u8>, CliError> {
    // This operation requires the ID to have a version
    if id.version.is_none() {
        return Err(fluvio_index::Error::MissingVersion.into());
    }

    // Download the package file from the package registry
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
    Ok(package_file)
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

pub fn install_bin<P: AsRef<Path>, B: AsRef<[u8]>>(
    bin_dir: P,
    name: &str,
    bytes: B,
) -> Result<(), CliError> {
    // Create bin_dir if it does not exist
    let bin_dir = bin_dir.as_ref();
    std::fs::create_dir_all(&bin_dir)?;

    // Install our package to `<bin_dir>/<name>`
    let install_path = bin_dir.join(name);
    std::fs::write(&install_path, bytes)?;

    // Mark our package as executable
    let status = std::process::Command::new("chmod")
        .arg("+x")
        .arg(install_path.as_os_str())
        .status()?;
    if !status.success() {
        return Err(CliError::Other(
            "Failed to make package executable".to_string(),
        ));
    }

    Ok(())
}
