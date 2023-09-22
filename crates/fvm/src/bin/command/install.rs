//! Initialization Command
//!
//! This command is used to initialize a new Fluvio Version Manager (FVM)
//! instance in the host system.

use std::fs::{File, create_dir, copy};
use std::io::Cursor;
use std::path::PathBuf;

use surf::{Client, StatusCode};
use tempfile::TempDir;

use color_eyre::eyre::Result;
use clap::Parser;
use color_eyre::owo_colors::OwoColorize;
use tracing::debug;
use url::Url;

use fluvio_hub_util::fvm::PackageSet;

use fluvio_version_manager::Error;
use fluvio_version_manager::common::{INFINYON_HUB_URL, FVM_PACKAGES_SET_DIR};
use fluvio_version_manager::install::{InstallTask, Version, fvm_bin_path, install_fvm, fvm_path};
use fluvio_version_manager::utils::file::{set_executable_mode, shasum256};
use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

/// The `install` command is responsible of installing the desired Package Set
#[derive(Debug, Parser)]
pub struct InstallOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
    /// Package Set to install
    #[arg(long, default_value = "default")]
    pkgset: String,
    /// Version to install
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
        if let Some(installed_fvm_path) = fvm_bin_path()? {
            debug!(path=?installed_fvm_path, "FVM is already installed");

            self.install_package().await?;
            return Ok(());
        }

        self.notify_info("Installing FVM...");
        install_fvm()?;

        self.notify_done("FVM installed successfully");
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
        self.download_artifacts(&tmp_dir, &pkgset).await?;

        self.notify_info("Downloaded artifacts with success!");
        let pkgset_dir = self
            .store_binaries(&install_task, &tmp_dir, &pkgset)
            .await?;

        self.notify_done(format!(
            "Stored binaries on {pkgset_dir}",
            pkgset_dir = pkgset_dir.display().italic()
        ));
        Ok(())
    }

    /// Downloads artifacts from the [`PackageSet`] into a local cache
    pub async fn download_artifacts(&self, tmp_dir: &TempDir, pkgset: &PackageSet) -> Result<()> {
        let client = Client::new();

        self.notify_info("Downloading artifacts...");
        for (idx, artf) in pkgset.artifacts.iter().enumerate() {
            let mut res = client.get(&artf.download_url).await.unwrap();

            if res.status() == StatusCode::Ok {
                self.notify_info(format!(
                    "Downloading artifact {idx}/{total}: {name}@{version}...",
                    idx = (idx + 1).to_string().bold(),
                    total = pkgset.artifacts.len().to_string().bold(),
                    name = artf.name.bold(),
                    version = pkgset.version.italic(),
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

            self.notify_warn(format!(
                "Failed to fetch artifact {idx}/{total}: {name}...",
                idx = (idx + 1).to_string().bold(),
                total = pkgset.artifacts.len().to_string().bold(),
                name = artf.name.bold()
            ));
        }

        Ok(())
    }

    /// Stores binaries in the FVM `pkgset` directory for future use
    pub async fn store_binaries(
        &self,
        install_task: &InstallTask,
        tmp_dir: &TempDir,
        pkgset: &PackageSet,
    ) -> Result<PathBuf> {
        let fvm_path = fvm_path()?;

        // FIXME: `PackageSet` Support for package set "name"
        let pkgset_dir = fvm_path.join(FVM_PACKAGES_SET_DIR).join("default");
        tracing::info!(?pkgset_dir, "Target directory for storing versions");

        if !pkgset_dir.exists() {
            tracing::info!(?pkgset_dir, "Creating PackageSet directory");
            create_dir(&pkgset_dir).map_err(|err| Error::Install(err.to_string()))?;
        }

        let pkgset_version_dir = pkgset_dir.join(&pkgset.version);
        tracing::info!(
            ?pkgset_version_dir,
            version = pkgset.version,
            "Target directory for storing version binaries"
        );

        if !pkgset_version_dir.exists() {
            tracing::info!(?pkgset_version_dir, "Creating directory for version");
            create_dir(&pkgset_version_dir).map_err(|err| Error::Install(err.to_string()))?;
        }

        for artifact in pkgset.artifacts.iter() {
            let binary_path = tmp_dir.path().join(&artifact.name);

            if binary_path.is_file() {
                let mut binary = File::open(binary_path)?;
                let shasum = shasum256(&binary)?;
                let upstream_shasum256 = install_task
                    .fetch_artifact_shasum(&artifact.name)
                    .await
                    .map_err(|err| Error::Install(err.to_string()))?;

                if shasum == upstream_shasum256 {
                    self.notify_info(format!(
                        "Checksums matched for package {} with shasum: {}",
                        artifact.name.bold(),
                        shasum.italic()
                    ));
                    set_executable_mode(&mut binary)?;
                    copy(
                        tmp_dir.path().join(&artifact.name),
                        pkgset_version_dir.join(&artifact.name),
                    )
                    .map_err(|err| Error::Install(err.to_string()))?;
                    continue;
                }

                self.notify_warn(format!(
                    "Artifact {} didnt matched upstream shasum {}. Skipping installation for this artifact...",
                    artifact.name, shasum
                ));
            }
        }

        Ok(pkgset_version_dir)
    }
}

impl Notify for InstallOpt {
    fn is_quiet(&self) -> bool {
        self.global_opts.quiet
    }
}
