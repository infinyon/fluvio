use std::sync::Arc;

use clap::ValueEnum;
use clap::Parser;
use common::installation::InstallationType;
use fluvio::config::ConfigFile;
use semver::Version;
use tracing::debug;

mod group;
mod spu;
mod start;
mod delete;
mod util;
mod check;
mod error;
mod diagnostics;
mod status;
mod shutdown;
mod upgrade;

use start::StartOpt;
use delete::DeleteOpt;
use check::CheckOpt;
use group::SpuGroupCmd;
use spu::SpuCmd;
use diagnostics::DiagnosticsOpt;
use status::StatusOpt;
use shutdown::ShutdownOpt;
use upgrade::UpgradeOpt;

pub use self::error::ClusterCliError;

use anyhow::Result;

use fluvio_extension_common as common;
use common::target::ClusterTarget;
use common::output::Terminal;
use fluvio_channel::{ImageTagStrategy, FLUVIO_IMAGE_TAG_STRATEGY};

pub(crate) const VERSION: &str = include_str!("../../../../VERSION");

/// Manage and view Fluvio clusters
#[derive(Debug, Parser)]
pub enum ClusterCmd {
    /// Install Fluvio cluster
    #[command(name = "start")]
    Start(Box<StartOpt>),

    /// Upgrades an already-started Fluvio cluster
    #[command(name = "upgrade")]
    Upgrade(Box<UpgradeOpt>),

    /// Uninstall a Fluvio cluster
    #[command(name = "delete")]
    Delete(DeleteOpt),

    /// Check that all requirements for cluster startup are met.
    ///
    /// This command is useful to check if user has all the required dependencies and permissions to run
    /// fluvio cluster.
    #[command(name = "check")]
    Check(CheckOpt),

    /// Manage and view Streaming Processing Units (SPUs)
    ///
    /// SPUs make up the part of a Fluvio cluster which is in charge
    /// of receiving messages from producers, storing those messages,
    /// and relaying them to consumers. This command lets you see
    /// the status of SPUs in your cluster.
    #[command(subcommand, name = "spu")]
    SPU(SpuCmd),

    /// Manage and view SPU Groups (SPGs)
    ///
    /// SPGs are groups of SPUs in a cluster which are managed together.
    #[command(subcommand, name = "spg")]
    SPUGroup(SpuGroupCmd),

    /// Collect anonymous diagnostic information to help with debugging
    #[command(name = "diagnostics")]
    Diagnostics(DiagnosticsOpt),

    /// Check the status of a Fluvio cluster
    #[command(name = "status")]
    Status(StatusOpt),

    /// Shutdown cluster processes without deleting data
    #[command(name = "shutdown")]
    Shutdown(ShutdownOpt),
}

impl ClusterCmd {
    /// process cluster commands
    pub async fn process<O: Terminal>(
        self,
        out: Arc<O>,
        platform_version: Version,
        target: ClusterTarget,
    ) -> Result<()> {
        match self {
            Self::Start(mut start) => {
                if let Ok(tag_strategy_value) = std::env::var(FLUVIO_IMAGE_TAG_STRATEGY) {
                    let tag_strategy = ImageTagStrategy::from_str(&tag_strategy_value, true)
                        .unwrap_or(ImageTagStrategy::Version);
                    match tag_strategy {
                        ImageTagStrategy::Version => {
                            debug!("Using image version: {}", VERSION);
                        }
                        ImageTagStrategy::VersionGit => {
                            let image_version = format!("{}-{}", VERSION, env!("GIT_HASH"));
                            debug!("Using image version: {:?}", &image_version);
                            start.k8_config.image_version = Some(image_version);
                        }
                        ImageTagStrategy::Git => {
                            debug!("Using developer image version: {}", env!("GIT_HASH"));
                            start.develop = true
                        }
                    }
                };

                start.process(platform_version, false).await?;
            }
            Self::Upgrade(mut upgrade) => {
                if let Ok(tag_strategy_value) = std::env::var(FLUVIO_IMAGE_TAG_STRATEGY) {
                    let tag_strategy = ImageTagStrategy::from_str(&tag_strategy_value, true)
                        .unwrap_or(ImageTagStrategy::Version);
                    match tag_strategy {
                        ImageTagStrategy::Version => {}
                        ImageTagStrategy::VersionGit => {
                            let image_version = format!("{}-{}", VERSION, env!("GIT_HASH"));
                            upgrade.start.k8_config.image_version = Some(image_version);
                        }
                        ImageTagStrategy::Git => upgrade.start.develop = true,
                    }
                };

                upgrade.process(platform_version).await?;
            }
            Self::Delete(uninstall) => {
                uninstall.process().await?;
            }
            Self::Check(check) => {
                check.process(platform_version).await?;
            }
            Self::SPU(spu) => {
                let fluvio = target.connect().await?;
                spu.process(out, &fluvio).await?;
            }
            Self::SPUGroup(group) => {
                let fluvio = target.connect().await?;
                group.process(out, &fluvio).await?;
            }
            Self::Diagnostics(opt) => {
                opt.process().await?;
            }
            Self::Status(status) => {
                status.process(target).await?;
            }
            Self::Shutdown(opt) => {
                opt.process().await?;
            }
        }

        Ok(())
    }
}

pub(crate) fn get_installation_type() -> Result<InstallationType, ClusterCliError> {
    let config = ConfigFile::load_default_or_new()?;
    Ok(InstallationType::load_or_default(
        config.config().current_cluster()?,
    ))
}

#[cfg(test)]
#[cfg(feature = "pcreate")]
mod test {
    use std::{env::temp_dir, fs, io::Write};

    use fluvio::config::ConfigFile;
    use fluvio_extension_common::pcreate::PCreateType;

    // use super::try_infer_pcreate_type;

    fn setup_config(toml: &str) -> ConfigFile {
        // generate a random file
        let random: String = std::iter::repeat_with(fastrand::alphanumeric)
            .take(10)
            .collect();
        let path = temp_dir().join(format!("fluvio_tests/inference_test{random}.toml"));
        fs::create_dir_all(path.parent().unwrap()).expect("failed to create temp directory");
        let mut file =
            fs::File::create(path.clone()).expect("failed to create file in temp directory");
        file.write_all(toml.as_bytes())
            .expect("failed to save file");

        let mut config = ConfigFile::load(Some(path.to_string_lossy().to_string()))
            .expect("failed to load config file");
        *config.mut_config() = toml::from_str(toml).expect("invalid toml");
        config
            .save()
            .expect("failed to save temp configuration file");

        config
    }

    #[test]
    fn test_infer_local_installation_type() {
        let mut config = setup_config(
            r#"version = "2.0"
current_profile = "local"

[profile.local]
cluster = "local"

[cluster.local]
endpoint = "localhost:9003"

[cluster.local.tls]
tls_policy = "disabled"
"#,
        );

        // TODOFIX
        // let installation = try_infer_installation_type(&mut config);
        // assert_eq!(installation, Some(InstallationType::Local));
    }

    #[test]
    fn test_infer_cloud_pcreate_type() {
        let mut config = setup_config(
            r#"version = "2.0"
current_profile = "cloud"

[profile.cloud]
cluster = "cloud"

[cluster.cloud]
endpoint = "endpoint.cloud"

[cluster.cloud.tls]
tls_policy = "disabled"
"#,
        );

        // TODO FIX
        // let installation = try_infer_installation_type(&mut config);
        // assert_eq!(installation, Some(InstallationType::Cloud));
    }

    #[test]
    fn test_infer_local_k8_installation_type() {
        let mut config = setup_config(
            r#"version = "2.0"
current_profile = "minikube"

[profile.minikube]
cluster = "minikube"

[cluster.minikube]
endpoint = "192.168.0.1:30003"

[cluster.minikube.tls]
tls_policy = "disabled"
"#,
        );

        // TODO FIX
        // let installation = try_infer_installation_type(&mut config);
        // assert_eq!(installation, Some(InstallationType::LocalK8));
    }
}
