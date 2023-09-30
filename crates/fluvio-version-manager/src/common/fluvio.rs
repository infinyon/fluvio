//! Utility function for Fluvio installation

use std::path::PathBuf;

use color_eyre::eyre::Result;

use super::home_dir;

/// Home Directory for Fluvio
pub const FLUVIO_HOME_DIR: &str = ".fluvio";

/// Fluvio Binary Name
pub const FLUVIO_BINARY_NAME: &str = "fluvio";

/// Retrieves the path to the `~/.fluvio` directory in the host system.
pub fn fluvio_path() -> Result<PathBuf> {
    let home = home_dir()?;
    let flv_path = home.join(FLUVIO_HOME_DIR);

    Ok(flv_path)
}

/// Retrieves the path to the `~/.fluvio/bin` directory in the host system
pub fn fluvio_binaries_path() -> Result<PathBuf> {
    let home = home_dir()?;
    let flv_path = home.join(FLUVIO_HOME_DIR).join("bin");

    Ok(flv_path)
}

/// Checks if Fluvio is installed. This is achieved by checking if the
/// binary is present in the `Fluvio` home directory.
pub fn fluvio_bin_path() -> Result<PathBuf> {
    let flv_path = fluvio_path()?;
    let flv_binary_path = flv_path.join("bin").join(FLUVIO_BINARY_NAME);

    Ok(flv_binary_path)
}

// pub fn create_fluvio_dir() -> Result<PathBuf> {
//   // Creates the directory `~/.fluvio` if doesn't exists
//   let flv_dir = fluvio_path()?;

//   if !flv_dir.exists() {
//       create_dir(&flv_dir).map_err(InstallError::ResolveCurrentExe)?;
//       tracing::debug!(?flv_dir, "Created Fluvio home directory");
//   }

//   // Attempts to create the binary crate
//   let flv_binary_dir = flv_dir.join("bin");
//   create_dir(flv_binary_dir).map_err(InstallError::ResolveCurrentExe)?;

//   Ok(flv_dir)
// }
