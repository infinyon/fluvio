use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use fluvio_types::config_file::SaveLoadConfig;
use semver::Version;
use tracing::debug;

use crate::cli::ClusterCliError;
use crate::progress::ProgressBarFactory;
use crate::start::local::LOCAL_CONFIG_PATH;
use crate::ClusterChecker;
use crate::LocalConfig;
use crate::LocalInstaller;
use crate::{InstallationType, cli::get_installation_type};
#[derive(Debug, Parser)]
pub struct ResumeOpt;

impl ResumeOpt {
    pub async fn process(self, platform_version: Version) -> Result<()> {
        let pb_factory = ProgressBarFactory::new(false);

        let pb = match pb_factory.create() {
            Ok(pb) => pb,
            Err(_) => {
                return Err(
                    ClusterCliError::Other("Failed to create progress bar".to_string()).into(),
                )
            }
        };
        let (installation_type, _config) = get_installation_type()?;
        debug!(?installation_type);

        let resume_result = match installation_type {
            InstallationType::Local | InstallationType::ReadOnly => {
                let resume = LocalResume {
                    pb_factory,
                    platform_version,
                };
                resume.resume().await
            }
            _ => {
                pb.println("❌ Resume is only implemented for local clusters.");
                Err(ClusterCliError::Other("Resume not implemented".to_string()).into())
            }
        };

        if let Some(err) = resume_result.err() {
            pb.println(format!("❌ Resume failed with {:#}", err));
        }

        Ok(())
    }
}

#[derive(Debug)]
struct LocalResume {
    pb_factory: ProgressBarFactory,
    platform_version: Version,
}

impl LocalResume {
    pub async fn resume(&self) -> Result<()> {
        self.preflight_check().await?;
        self.resume_previous_config().await
    }

    async fn resume_previous_config(&self) -> Result<()> {
        let local_conf = match LOCAL_CONFIG_PATH.as_ref() {
            None => {
                return Err(ClusterCliError::Other(
                    "Configuration file for local cluster not found from previous run".to_string(),
                )
                .into())
            }
            Some(local_config_path) => LocalConfig::load_from(local_config_path),
        }
        .with_context(|| "Unable to load configuration file for local cluster")?;

        let installer = LocalInstaller::from_config(local_conf);
        _ = installer.install_only().await?;
        Ok(())
    }

    async fn preflight_check(&self) -> Result<()> {
        ClusterChecker::empty()
            .without_installed_local_cluster()
            .with_local_cluster_version(self.platform_version.clone())
            .run(&self.pb_factory, false)
            .await?;
        Ok(())
    }
}
