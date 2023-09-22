use std::fs::{copy, read_dir};
use std::path::{PathBuf, Path};

use tracing::info;

use crate::common::FLUVIO_HOME_DIR;
use crate::{Result, Error};

/// Retrieves the absolute path to `~/.fluvio/bin/` for the host
pub fn fluvio_bin_path() -> Result<PathBuf> {
    let Some(home_dir) = dirs::home_dir() else {
        return Err(Error::HomeDirNotFound);
    };

    let fluvio_bin = home_dir.join(FLUVIO_HOME_DIR).join("bin");

    Ok(fluvio_bin)
}

/// Replaces binaries in the `pkgset` path with the binaries in `target` path
pub fn overwrite_binaries(pkgset_path: &Path, target_path: &Path) -> Result<()> {
    info!(
        "Overwriting binaries from {} to {}",
        pkgset_path.display(),
        target_path.display()
    );

    let binaries = read_dir(target_path)?;

    for binary in binaries {
        let binary = binary?;
        let binary_name = binary.file_name();

        if let Some(binary_name) = binary_name.to_str() {
            let pkgset_binary_path = pkgset_path.join(binary_name);
            let binary_path = binary.path();

            info!(
                "Overwriting binary {} with {}",
                binary_path.display(),
                pkgset_binary_path.display(),
            );

            // FIXME: We need better handling of errors here and investigate
            // why these scenarios occur
            if copy(&pkgset_binary_path, &binary_path).is_ok() {
                info!("Binary {} overwritten", binary_path.display());
            } else {
                info!("Binary {} not ovrwritten", binary_path.display());
            }
        }
    }

    Ok(())
}
