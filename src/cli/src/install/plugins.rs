use structopt::StructOpt;
use fluvio_index::{PackageId, Target, HttpAgent};
use crate::CliError;
use crate::install::{fetch_latest_version, fetch_package_file, fluvio_bin_dir, install_bin};

#[derive(StructOpt, Debug)]
pub struct InstallOpt {
    id: PackageId,
}

impl InstallOpt {
    pub async fn process(self) -> Result<String, CliError> {
        let target: Target = fluvio_index::PACKAGE_TARGET.parse()?;
        let agent = HttpAgent::default();

        // If a version is given in the package ID, use it. Otherwise, use latest
        let id = match self.id.version.as_ref() {
            Some(_) => {
                println!("⏳ Downloading package with provided version: {}...", &self.id);
                self.id
            },
            None => {
                let mut id = self.id;
                println!("🎣 Fetching latest version for package: {}...", &id);
                let version = fetch_latest_version(&agent, &id, target).await?;
                id.version = Some(version);
                println!("⏳ Downloading package with latest version: {}...", &id);
                id
            },
        };

        // Download the package file from the package registry
        let package_file = fetch_package_file(&agent, &id, target).await?;
        println!("🔑 Downloaded and verified package file");

        // Install the package to the ~/.fluvio/bin/ dir
        let fluvio_dir = fluvio_bin_dir()?;
        install_bin(&fluvio_dir, id.name.as_str(), &package_file)?;
        Ok(format!("✅ Successfully installed ~/.fluvio/bin/{}", &id.name))
    }
}
