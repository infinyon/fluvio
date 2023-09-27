//! Initialization Command
//!
//! This command is used to initialize a new Fluvio Version Manager (FVM)
//! instance in the host system.

use std::fs::{File, create_dir, copy};
use std::io::Cursor;
use std::path::PathBuf;
use std::str::FromStr;

use surf::{Client, StatusCode};
use tempfile::TempDir;

use color_eyre::eyre::Result;
use clap::Parser;
use color_eyre::owo_colors::OwoColorize;

use url::Url;

use fluvio_hub_util::fvm::{PackageSet, STABLE_VERSION_CHANNEL, DEFAULT_PKGSET, Channel};

use fluvio_version_manager::Error;
use fluvio_version_manager::common::{INFINYON_HUB_URL, FVM_PACKAGES_SET_DIR};
use fluvio_version_manager::install::{fvm_bin_path, fvm_path, create_fluvio_dir, fluvio_binaries_path};
use fluvio_version_manager::package::InstallTask;
use fluvio_version_manager::utils::file::{set_executable_mode, shasum256};
use fluvio_version_manager::utils::notify::Notify;

use crate::GlobalOptions;

/// The `install` command is responsible of installing the desired Package Set
#[derive(Debug, Parser)]
pub struct InstallOpt {
    #[command(flatten)]
    global_opts: GlobalOptions,
    /// Version to install
    #[arg(index = 1, default_value = STABLE_VERSION_CHANNEL)]
    version: String,
    /// Registry used to fetch Fluvio Versions
    #[clap(long, hide = true, default_value = INFINYON_HUB_URL)]
    registry: Url,
}

impl InstallOpt {
    /// Processes install the specified Package Set. If this is the first time
    /// FVM `install` command is being used, or the local FVM isn`t installed,
    /// then it also installs FVM.
    pub async fn process(&self) -> Result<()> {
        if fvm_bin_path()?.is_some() {
            if fluvio_binaries_path()?.exists() {
                self.install_package().await?;
                self.notify_help(format!(
                    "Use {} to switch to this version",
                    format!("fvm switch {}", self.version).bold()
                ));
                return Ok(());
            }

            self.notify_info(
                "No previous Fluvio installation found. Preparing Fluvio workspace...",
            );
            create_fluvio_dir()?;

            self.notify_info("Proceeding to install Fluvio...");
            self.install_package().await?;

            return Ok(());
        }

        self.notify_warn("Aborting installation due to missing FVM installation");
        self.notify_help("Try running `fvm self install` and then retry this command");

        Ok(())
    }

    ///  Performs the installation of the specified `PackageSet`
    async fn install_package(&self) -> Result<()> {
        let channel = Channel::from_str(&self.version)?;
        let install_task =
            InstallTask::new(self.registry.clone(), DEFAULT_PKGSET.to_string(), channel);
        let pkgset = install_task.fetch_pkgset().await?;

        self.notify_info(format!(
            "Found packages for {pkgset}@{version}",
            pkgset = install_task.pkgset.bold(),
            version = pkgset.version.bold(),
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
        for artf in pkgset.artifacts.iter() {
            let mut res = client.get(&artf.download_url).await.unwrap();

            if res.status() == StatusCode::Ok {
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

            tracing::warn!(artifact = artf.name, "Failed to fetch artifact",);
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
                    self.notify_done(format!(
                        "{}@{} is ready for use",
                        artifact.name.bold(),
                        self.version.italic()
                    ));
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
