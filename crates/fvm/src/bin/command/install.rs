//! Initialization Command
//!
//! This command is used to initialize a new Fluvio Version Manager (FVM)
//! instance in the host system.

use std::fs::File;
use std::io::Cursor;

use surf::{Client, StatusCode};
use tempfile::TempDir;

use color_eyre::eyre::Result;
use clap::Parser;
use color_eyre::owo_colors::OwoColorize;
use tracing::debug;
use url::Url;

use fluvio_hub_util::fvm::{DEFAULT_PACKAGE_SET, PackageSet};

use fluvio_version_manager::Error;
use fluvio_version_manager::common::INFINYON_HUB_URL;
use fluvio_version_manager::install::{InstallTask, Version};
use fluvio_version_manager::setup::{is_fvm_installed, install_fvm};
use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

/// The `init` command is responsible of preparing the workspace for FVM.
#[derive(Debug, Parser)]
pub struct InstallOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
    /// Package Set to install
    #[arg(long, default_value = DEFAULT_PACKAGE_SET)]
    pkgset: String,
    /// Version to install
    // #[arg(long, default_value = STABLE_CHANNEL)]
    #[arg(long, default_value = "0.10.14")]
    version: Version,
    /// Registry used to fetch Fluvio Versions
    #[clap(long, default_value = INFINYON_HUB_URL)]
    registry: Url,
}

impl InstallOpt {
    /// Processes install the specified Package Set. If this is the first time
    /// FVM `install` command is being used, or the local FVM isn`t installed,
    /// then it also installs FVM.
    pub async fn process(&self) -> Result<()> {
        if let Some(installed_fvm_path) = is_fvm_installed()? {
            debug!(path=?installed_fvm_path, "FVM is already installed");

            self.install_package().await?;
            return Ok(());
        }

        self.notify_info("Installing FVM...");
        install_fvm()?;

        self.notify_success("FVM installed successfully");
        self.install_package().await?;
        Ok(())
    }

    ///  Performs the installation of the specified `PackageSet`
    async fn install_package(&self) -> Result<()> {
        let install_task = InstallTask::new(
            self.registry.clone(),
            self.pkgset.clone(),
            self.version.clone(),
        );

        tracing::info!(?install_task, "Created InstallTask");
        self.notify_info(format!(
            "Installing Package Set {pkgset}@{version}...",
            pkgset = self.pkgset.bold(),
            version = self.version.bold()
        ));

        let pkgset = install_task.fetch_pkgset().await?;
        self.notify_info(format!(
            "Found {arts} packages in {pkgset}@{version}...",
            arts = pkgset.artifacts.len(),
            pkgset = self.pkgset.bold(),
            version = self.version.bold()
        ));

        let tmp_dir = TempDir::new().map_err(|err| Error::CreateTempDir(err.to_string()))?;
        self.download_artifacts(&tmp_dir, &install_task, &pkgset)
            .await?;
        Ok(())
    }

    /// Downloads artifacts from the [`PackageSet`] into a local cache
    pub async fn download_artifacts(
        &self,
        tmp_dir: &TempDir,
        install_task: &InstallTask,
        pkgset: &PackageSet,
    ) -> Result<()> {
        let client = Client::new();

        self.notify_info("Downloading artifacts...");
        for (idx, artf) in pkgset.artifacts.iter().enumerate() {
            let mut res = client
                .get(&artf.download_url)
                .await
                .map_err(|err| Error::ArtifactDownload(install_task.to_owned(), err))?;

            if res.status() == StatusCode::Ok {
                self.notify_info(format!(
                    "Downloading artifact {idx}/{total}: {name}...",
                    idx = (idx + 1).to_string().bold(),
                    total = pkgset.artifacts.len().to_string().bold(),
                    name = artf.name.bold()
                ));

                let out_path = tmp_dir.path().join(&artf.name);
                let mut file = File::create(&out_path).unwrap();
                let mut buf = Cursor::new(res.body_bytes().await.unwrap());

                std::io::copy(&mut buf, &mut file).unwrap();
                tracing::info!(
                    "Artifact downloaded: {} at {:?}",
                    artf.name,
                    out_path.display()
                );
                continue;
            }

            self.notify_warning(format!(
                "Failed to find artifact {idx}/{total}: {name}...",
                idx = (idx + 1).to_string().bold(),
                total = pkgset.artifacts.len().to_string().bold(),
                name = artf.name.bold()
            ));
        }

        Ok(())
    }
}

impl Notify for InstallOpt {
    fn command(&self) -> &'static str {
        "install"
    }

    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
