use std::fs::File;
use std::io::{ErrorKind, Error as IoError};
use std::path::{Path, PathBuf};
use tracing::{debug, instrument};
use semver::Version;
use fluvio_index::{HttpAgent, PackageId, Target, WithVersion, PackageVersion};
use crate::FLUVIO_EXTENSIONS_DIR;
use fluvio_types::defaults::CLI_CONFIG_PATH;
use crate::error::{Result, CliError};

pub const FLUVIO_DIR: &str = "FLUVIO_DIR";

fn fluvio_base_dir() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var(FLUVIO_DIR) {
        // Assume this is like `~/.fluvio
        let path = PathBuf::from(dir);
        return fluvio_base_dir_create(path);
    }
    let home =
        home::home_dir().ok_or_else(|| IoError::new(ErrorKind::NotFound, "Homedir not found"))?;
    let path = home.join(CLI_CONFIG_PATH);

    fluvio_base_dir_create(path)
}

pub fn fluvio_bin_dir() -> Result<PathBuf> {
    Ok(fluvio_base_dir()?.join("bin"))
}

fn fluvio_base_dir_create(path: PathBuf) -> Result<PathBuf> {
    if !path.exists() {
        // Create the base dir if it doesn't exist yet (#718)
        std::fs::create_dir_all(&path)?
    }
    Ok(path)
}

pub fn fluvio_extensions_dir() -> Result<PathBuf> {
    // Check if FLUVIO_EXTENSIONS_DIR exists for extensions location
    if let Ok(dir_path) = std::env::var(FLUVIO_EXTENSIONS_DIR) {
        Ok(dir_path.into())
    } else {
        let base_dir = fluvio_base_dir()?;
        let path = base_dir.join("extensions");

        if !path.exists() {
            std::fs::create_dir(&path)?;
        }
        Ok(path)
    }
}

// Think about adding check in bin dir for extensions.
// Add to count by skipping anything that starts with `fluvio-`
// I'm not sure if this is how things get added to the CLI though
pub fn get_extensions() -> Result<Vec<PathBuf>> {
    use std::fs;
    let mut extensions = Vec::new();
    let fluvio_dir = fluvio_extensions_dir()?;
    if let Ok(entries) = fs::read_dir(&fluvio_dir) {
        for entry in entries.flatten() {
            let is_plugin = entry.file_name().to_string_lossy().starts_with("fluvio-");
            if is_plugin {
                extensions.push(entry.path());
            }
        }
    }
    Ok(extensions)
}

/// Fetches the latest version of the package with the given ID
#[instrument(
    skip(agent, target, id),
    fields(%target, id = %id.pretty())
)]
pub async fn fetch_latest_version<T>(
    agent: &HttpAgent,
    id: &PackageId<T>,
    target: &Target,
    prerelease: bool,
) -> Result<Version> {
    let request = agent.request_package(id)?;
    debug!(
        uri = %request.uri(),
        "Requesting package manifest:",
    );
    let response = crate::http::execute(request).await?;
    let body = crate::http::read_to_end(response).await?;
    let package = agent.package_from_response(&body).await?;
    let latest_release = package.latest_release_for_target(target, prerelease)?;
    debug!(release = ?latest_release, "Latest release for package:");
    if !latest_release.target_exists(target) {
        return Err(fluvio_index::Error::MissingTarget(target.clone()).into());
    }
    Ok(latest_release.version.clone())
}

/// Downloads and verifies a package file via it's versioned ID and target
#[instrument(
    skip(agent, id, target),
    fields(%target, %id)
)]
pub async fn fetch_package_file(
    agent: &HttpAgent,
    id: &PackageId<WithVersion>,
    target: &Target,
) -> Result<Vec<u8>> {
    // If the PackageVersion is a tag, try to resolve it to a semver::Version
    let version = match id.version() {
        PackageVersion::Semver(version) => version.clone(),
        PackageVersion::Tag(tag) => {
            let tag_request = agent.request_tag(id, tag)?;
            let tag_response = crate::http::execute(tag_request).await?;
            let body = crate::http::read_to_end(tag_response).await?;
            agent.tag_version_from_response(tag, &body).await?
        }
        _ => {
            return Err(
                fluvio_index::Error::Other("unknown PackageVersion type".to_string()).into(),
            )
        }
    };

    // Download the package file from the package registry
    let download_request = agent.request_release_download(id, &version, target)?;
    debug!(uri = %download_request.uri(), "Requesting package download:");
    let response = crate::http::execute(download_request).await?;
    if !response.status().is_success() {
        return Err(CliError::PackageNotFound {
            package: id.clone().into_unversioned(),
            version: version.clone(),
            target: target.clone(),
        });
    }

    let package_file = crate::http::read_to_end(response).await?;

    // Download the package checksum from the package registry
    let checksum_request = agent.request_release_checksum(id, &version, target)?;
    let response = crate::http::execute(checksum_request).await?;
    let body = crate::http::read_to_end(response).await?;
    let package_checksum = String::from_utf8_lossy(&body);

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

pub fn install_bin<P: AsRef<Path>, B: AsRef<[u8]>>(bin_path: P, bytes: B) -> Result<()> {
    use std::io::Write as _;

    let bin_path = bin_path.as_ref();

    // Create directories to bin_path if they do not exist
    let parent = bin_path
        .parent()
        .ok_or_else(|| IoError::new(ErrorKind::NotFound, "parent directory not found"))?;
    std::fs::create_dir_all(&parent)?;

    // Create a temporary dir to write file to
    let tmp_dir = tempdir::TempDir::new_in(parent, "fluvio-tmp")?;

    // Write bin to temporary file
    let tmp_path = tmp_dir.path().join("fluvio-exe-tmp");
    let mut tmp_file = File::create(&tmp_path)?;
    tmp_file.write_all(bytes.as_ref())?;

    // Mark the file as executable
    make_executable(&mut tmp_file)?;

    // Rename (atomic move on unix) temp file to destination
    std::fs::rename(&tmp_path, &bin_path)?;

    Ok(())
}

#[cfg(unix)]
fn make_executable(file: &mut File) -> std::result::Result<(), IoError> {
    use std::os::unix::fs::PermissionsExt;

    // Add u+rwx mode to the existing file permissions, leaving others unchanged
    let mut permissions = file.metadata()?.permissions();
    let mut mode = permissions.mode();
    mode |= 0o700;
    permissions.set_mode(mode);

    file.set_permissions(permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn make_executable(_file: &mut File) -> std::result::Result<(), IoError> {
    Ok(())
}

pub fn install_println<S: AsRef<str>>(string: S) {
    if std::env::var("FLUVIO_BOOTSTRAP").is_ok() {
        println!("\x1B[1;34mfluvio:\x1B[0m {}", string.as_ref());
    } else {
        println!("{}", string.as_ref());
    }
}
