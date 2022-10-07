use clap::Parser;
use fluvio_index::{PackageId, HttpAgent, MaybeVersion};
use super::error_convert;
use tracing::debug;

use crate::Result;
use fluvio_cli_common::error::CliError as CommonCliError;
use fluvio_cli_common::install::{
    fetch_latest_version, fetch_package_file, fluvio_extensions_dir, install_bin, install_println,
    fluvio_bin_dir,
};
use crate::install::update::{
    check_update_required, prompt_required_update, check_update_available, prompt_available_update,
};

#[derive(Parser, Debug)]
pub struct InstallOpt {
    /// The ID of a package to install, e.g. "fluvio/fluvio-cloud".
    package: PackageId<MaybeVersion>,
    /// Used for testing. Specifies alternate package location, e.g. "test/"
    #[clap(hide = true, long)]
    prefix: Option<String>,
    /// Install the latest prerelease rather than the latest release
    ///
    /// If the package ID contains a version (e.g. `fluvio/fluvio:0.6.0`), this is ignored
    #[clap(long)]
    pub develop: bool,
}

impl InstallOpt {
    pub async fn process(self) -> Result<()> {
        let agent = match &self.prefix {
            Some(prefix) => HttpAgent::with_prefix(prefix)?,
            None => HttpAgent::default(),
        };

        // Before any "install" type command, check if the CLI needs updating.
        // This may be the case if the index schema has updated.
        let require_update = check_update_required(&agent).await?;
        if require_update {
            prompt_required_update(&agent).await?;
            return Ok(());
        }

        let result = self.install_plugin(&agent).await;
        match result {
            Ok(_) => (),
            Err(crate::CliError::IndexError(fluvio_index::Error::MissingTarget(target))) => {
                install_println(format!(
                    "❕ Package '{}' is not available for target {}, skipping",
                    self.package.name(),
                    target
                ));
                install_println("❕ Consider filing an issue to add support for this platform using the link below! 👇");
                install_println(format!(
                    "❕   https://github.com/infinyon/fluvio/issues/new?title=Support+fluvio-cloud+on+target+{}",
                    target
                ));
                return Ok(());
            }
            Err(e) => return Err(e),
        }

        // After any "install" command, check if the CLI has an available update,
        // i.e. one that is not required, but present.
        let update_result = check_update_available(&agent, false).await;
        if let Ok(Some(latest_version)) = update_result {
            prompt_available_update(&latest_version);
        }
        Ok(())
    }

    async fn install_plugin(&self, agent: &HttpAgent) -> Result<()> {
        let target = fluvio_index::package_target()?;

        // If a version is given in the package ID, use it. Otherwise, use latest
        let id = match self.package.maybe_version() {
            Some(version) => {
                install_println(format!(
                    "⏳ Downloading package with provided version: {}...",
                    &self.package
                ));
                let version = version.clone();
                self.package.clone().into_versioned(version)
            }
            None => {
                let id = &self.package;
                install_println(format!("🎣 Fetching latest version for package: {}...", id));
                let version = fetch_latest_version(agent, id, &target, self.develop).await?;
                let id = id.clone().into_versioned(version.into());
                install_println(format!(
                    "⏳ Downloading package with latest version: {}...",
                    id
                ));
                id
            }
        };

        // Download the package file from the package registry
        let package_result = fetch_package_file(agent, &id, &target).await;
        let package_file = match package_result {
            Ok(pf) => pf,
            Err(CommonCliError::PackageNotFound {
                package,
                version,
                target,
            }) => {
                install_println(format!(
                    "❕ Package {} is not published at {} for {}, skipping",
                    package, version, target
                ));
                return Ok(());
            }
            Err(other) => return Err(error_convert(other)),
        };
        install_println("🔑 Downloaded and verified package file");

        // Install the package to the ~/.fluvio/bin/ dir
        // If the plugin name doesn't start with `fluvio-`, then install it to the bin dir
        let fluvio_dir = if id.name().to_string().starts_with("fluvio-") {
            fluvio_extensions_dir()?
        } else {
            fluvio_bin_dir()?
        };
        debug!("{fluvio_dir:#?}");

        let package_filename = if target.to_string().contains("windows") {
            format!("{}.exe", id.name().as_str())
        } else {
            id.name().to_string()
        };
        let package_path = fluvio_dir.join(&package_filename);
        install_bin(&package_path, &package_file)?;

        Ok(())
    }
}
