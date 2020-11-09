use structopt::StructOpt;
use fluvio_index::{PackageId, Target, HttpAgent};

use crate::CliError;
use crate::install::{fetch_latest_version, fetch_package_file, fluvio_bin_dir, install_bin, install_println};
use crate::install::update::{
    check_update_required, prompt_required_update, check_update_available, prompt_available_update,
};

#[derive(StructOpt, Debug)]
pub struct InstallOpt {
    id: PackageId,
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
        let target: Target = fluvio_index::PACKAGE_TARGET.parse()?;

        // If a version is given in the package ID, use it. Otherwise, use latest
        let id = match self.id.version.as_ref() {
            Some(_) => {
                install_println(format!("⏳ Downloading package with provided version: {}...", &self.id));
                self.id
            }
            None => {
                let mut id = self.id;
                install_println(format!("🎣 Fetching latest version for package: {}...", &id));
                let version = fetch_latest_version(agent, &id, target).await?;
                id.version = Some(version);
                install_println(format!("⏳ Downloading package with latest version: {}...", &id));
                id
            }
        };

        // Download the package file from the package registry
        let package_file = fetch_package_file(agent, &id, target).await?;
        install_println(format!("🔑 Downloaded and verified package file"));

        // Install the package to the ~/.fluvio/bin/ dir
        let fluvio_dir = fluvio_bin_dir()?;
        install_bin(&fluvio_dir, id.name.as_str(), &package_file)?;
        install_println(format!("✅ Successfully installed ~/.fluvio/bin/{}", &id.name));

        Ok("".to_string())
    }
}
