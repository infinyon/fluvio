use clap::{Parser, ValueEnum};
use fluvio_channel::ImageTagStrategy;
use fluvio_cli_common::FLUVIO_IMAGE_TAG_STRATEGY;
use fluvio_extension_common::installation::InstallationType;
use semver::Version;
use anyhow::{bail, Result};
use tracing::debug;

use crate::{
    cli::{
        get_installation_type, 
        shutdown::ShutdownOpt,
        options::{
            ClusterConnectionOpts,
            K8Install
        },
    }, 
    ClusterConfig, 
    LocalConfig
};

use super::VERSION;

#[derive(Debug, Parser)]
pub struct UpgradeOpt {
    #[clap(flatten)]
    pub connection_config: ClusterConnectionOpts,

    #[clap(flatten)]
    pub k8_config: K8Install,
}

impl UpgradeOpt {
    pub async fn process(mut self, platform_version: Version) -> Result<()> {
        let (installation_type, config) = get_installation_type()?;
        debug!(?installation_type);

        // TODO: Overrider k8s image config
        match get_image_override() {
            ImageTag::Develop => {
                // upgrade.start.develop = true
            },
            ImageTag::GitVersion(image_version) => {
                self.k8_config.image_version = Some(image_version);
            },
            _ => {},
        };

        match installation_type {
            InstallationType::K8 => {
                process_k8(self, platform_version, /* TODO: Why develop here is true? */ true).await?;
            }

            InstallationType::Local | InstallationType::LocalK8 | InstallationType::ReadOnly => {
                ShutdownOpt.process().await?;
                process_local(self, platform_version).await?;
            }

            InstallationType::Cloud => {
                let profile = config.config().current_profile_name().unwrap_or("none");
                bail!("Fluvio cluster upgrade does not operate on cloud cluster \"{profile}\", use 'fluvio cloud ...' commands")
            }
            other => bail!("upgrade command is not supported for {other} installation type"),
        };

        Ok(())
    }
}

enum ImageTag {
    Develop,
    GitVersion(String),
    Default
}

fn get_image_override() -> ImageTag {
    if let Ok(tag_strategy_value) = std::env::var(FLUVIO_IMAGE_TAG_STRATEGY) {
        let tag_strategy = ImageTagStrategy::from_str(&tag_strategy_value, true)
            .unwrap_or(ImageTagStrategy::Version);
        match tag_strategy {
            ImageTagStrategy::Version => ImageTag::Default,
            ImageTagStrategy::VersionGit => {
                let image_version = format!("{}-{}", VERSION, env!("GIT_HASH"));
                ImageTag::GitVersion(image_version)
            },
            ImageTagStrategy::Git => ImageTag::Develop,
        }
    } else  {
        ImageTag::Default
    }
}

async fn process_k8(opt: UpgradeOpt, platform_version: Version, develop: bool) -> Result<()> {
    let mut builder = ClusterConfig::builder(platform_version);
    if develop {
        builder.development()?;
    }
    
    builder
        .append_connection_options(opt.connection_config)?
        .upgrade(true)
        .build_and_start(false)
        .await
}

async fn process_local(opt: UpgradeOpt, platform_version: Version) -> Result<()> {
    LocalConfig::builder(platform_version)
        .append_connection_options(opt.connection_config)?
        .build_and_start(false, true)
        .await
}
