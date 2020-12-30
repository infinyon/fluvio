use structopt::StructOpt;
use fluvio_index::{PackageId, HttpAgent, MaybeVersion};

use crate::CliError;
use crate::install::{
    fetch_latest_version, fetch_package_file, fluvio_extensions_dir, install_bin, install_println,
};
use crate::install::update::{
    check_update_required, prompt_required_update, check_update_available, prompt_available_update,
};

#[derive(StructOpt, Debug)]
pub struct InstallOpt {
    /// The ID of a package to install, e.g. "fluvio/fluvio-cloud".
    package: PackageId<MaybeVersion>,
    /// Used for testing. Specifies alternate package location, e.g. "test/"
    #[structopt(hidden = true, long)]
    prefix: Option<String>,
}

impl InstallOpt {
    pub async fn process(self) -> Result<String, CliError> {
        let agent = match &self.prefix {
            Some(prefix) => HttpAgent::with_prefix(prefix)?,
            None => HttpAgent::default(),
        };

        // Before any "install" type command, check if the CLI needs updating.
        // This may be the case if the index schema has updated.
        let require_update = check_update_required(&agent).await?;
        if require_update {
            prompt_required_update(&agent).await?;
            return Ok("".to_string());
        }

        self.install_plugin(&agent).await?;

        // After any 'install' command, check if the CLI has an available update,
        // i.e. one that is not required, but present.
        let update_available = check_update_available(&agent).await?;
        if update_available {
            prompt_available_update(&agent).await?;
        }

        Ok("".to_string())
    }

    async fn install_plugin(self, agent: &HttpAgent) -> Result<String, CliError> {
        let target = fluvio_index::package_target()?;

        // If a version is given in the package ID, use it. Otherwise, use latest
        let id = match self.package.maybe_version() {
            Some(version) => {
                install_println(format!(
                    "⏳ Downloading package with provided version: {}...",
                    &self.package
                ));
                let version = version.clone();
                self.package.into_versioned(version)
            }
            None => {
                let id = self.package;
                install_println(format!(
                    "🎣 Fetching latest version for package: {}...",
                    &id
                ));
                let version = fetch_latest_version(agent, &id, target).await?;
                let id = id.into_versioned(version);
                install_println(format!(
                    "⏳ Downloading package with latest version: {}...",
                    &id
                ));
                id
            }
        };

        // Download the package file from the package registry
        let package_file = fetch_package_file(agent, &id, target).await?;
        install_println("🔑 Downloaded and verified package file");

        // Install the package to the ~/.fluvio/bin/ dir
        let fluvio_dir = fluvio_extensions_dir()?;
        let package_path = fluvio_dir.join(id.name.as_str());
        install_bin(&package_path, &package_file)?;

        Ok("".to_string())
    }
}
