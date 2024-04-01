use anyhow::Result;
use clap::Parser;
use fluvio_types::config_file::SaveLoadConfig;
use semver::Version;
use tracing::debug;

use crate::cli::ClusterCliError;
use crate::progress::ProgressBarFactory;
use crate::start::local::LOCAL_CONFIG_PATH;
use crate::LocalConfig;
use crate::LocalInstaller;
use crate::{InstallationType, cli::get_installation_type};
#[derive(Debug, Parser)]
pub struct ResumeOpt;

impl ResumeOpt {
    pub async fn process(self, _platform_version: Version) -> Result<()> {
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

        match installation_type {
            InstallationType::Local | InstallationType::ReadOnly => {
                Self::resume().await?;
            }
            _ => {
                pb.println("❌ Resume is only implemented for local clusters.");
            }
        };
        Ok(())
    }

    async fn resume() -> Result<()> {
        let local_conf = match LOCAL_CONFIG_PATH.as_ref() {
            None => {
                return Err(ClusterCliError::Other("Can't find older config".to_string()).into())
            }
            Some(local_config_path) => LocalConfig::load_from(local_config_path),
        }?;

        let installer = LocalInstaller::from_config(local_conf);
        _ = installer.install().await?;
        Ok(())
    }
}
