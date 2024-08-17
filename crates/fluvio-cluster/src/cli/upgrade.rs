use clap::Parser;
use color_eyre::owo_colors::OwoColorize;
use colored::Colorize;
use fluvio_extension_common::installation::InstallationType;
use fluvio_sc_schema::{
    mirror::MirrorSpec, partition::PartitionSpec, smartmodule::SmartModuleSpec, spg::SpuGroupSpec,
    spu::SpuSpec, store::NameSpace, tableformat::TableFormatSpec, topic::TopicSpec,
};
use fluvio_stream_dispatcher::metadata::{local::LocalMetadataStorage, MetadataClient};
use fluvio_types::config_file::SaveLoadConfig;
use semver::Version;
use anyhow::{anyhow, Result, bail};
use tracing::debug;

use crate::{
    cli::{get_installation_type, shutdown::ShutdownOpt, ClusterCliError},
    progress::ProgressBarFactory,
    render::ProgressRenderer,
    start::local::{
        DEFAULT_DATA_DIR, DEFAULT_METADATA_SUB_DIR, DEFAULT_RUNNER_PATH, LOCAL_CONFIG_PATH,
    },
    LocalConfig,
};

use super::start::StartOpt;

#[derive(Debug, Parser)]
pub struct UpgradeOpt {
    /// Force upgrade without confirmation
    #[clap(short, long)]
    pub force: bool,
    #[clap(flatten)]
    pub start: StartOpt,
}

impl UpgradeOpt {
    pub async fn process(mut self, platform_version: Version) -> Result<()> {
        let (installation_type, config) = get_installation_type()?;
        debug!(?installation_type);
        if let Some(requested) = self.start.installation_type.get() {
            if installation_type != requested {
                bail!("It is not allowed to change installation type during cluster upgrade. Current: {installation_type}, requested: {requested}");
            }
        } else {
            self.start.installation_type.set(installation_type.clone());
        }

        if !self.force {
            let prompt = dialoguer::Confirm::new()
                .with_prompt(format!(
                    "Upgrade Local Fluvio cluster to version {}?",
                    platform_version
                ))
                .interact()?;

            if !prompt {
                println!("Upgrade cancelled");
                return Ok(());
            }
        }

        match installation_type {
            InstallationType::K8 => {
                self.start.process(platform_version, true).await?;
            }
            InstallationType::Local | InstallationType::LocalK8 | InstallationType::ReadOnly => {
                let pb_factory = ProgressBarFactory::new(false);

                let pb = match pb_factory.create() {
                    Ok(pb) => pb,
                    Err(_) => {
                        return Err(ClusterCliError::Other(
                            "Failed to create progress bar".to_string(),
                        )
                        .into())
                    }
                };
                ShutdownOpt.process().await?;
                if let Err(err) = self.upgrade_local_cluster(&pb, platform_version).await {
                    pb.println(format!("💔 {}", err.to_string().red()));
                }
                pb.finish_and_clear();
            }
            InstallationType::Cloud => {
                let profile = config.config().current_profile_name().unwrap_or("none");
                bail!("Fluvio cluster upgrade does not operate on cloud cluster \"{profile}\", use 'fluvio cloud ...' commands")
            }
            other => bail!("upgrade command is not supported for {other} installation type"),
        };

        Ok(())
    }

    async fn upgrade_local_cluster(
        &self,
        pb: &ProgressRenderer,
        platform_version: Version,
    ) -> Result<()> {
        let local_config_path = LOCAL_CONFIG_PATH
            .as_ref()
            .ok_or(anyhow!("Local config path not set"))?;

        let data_path = DEFAULT_DATA_DIR
            .as_ref()
            .ok_or(anyhow!("Data path not set"))?;

        let metadata_path = data_path.join(DEFAULT_METADATA_SUB_DIR);
        let client = LocalMetadataStorage::new(metadata_path);

        check_all_metadata(pb, &client)
            .await
            .map_err(|err| anyhow!("Failed to check metadata: {err}"))?;

        let mut local_config = LocalConfig::load_from(local_config_path)
            .map_err(|err| anyhow!("Failed to load local config: {err}"))?
            .evolve();

        pb.println(format!(
            "🚀 {}",
            format!("Upgrading Local Fluvio cluster to {}", platform_version).bold(),
        ));

        let config = local_config
            .platform_version(platform_version.clone())
            .launcher(DEFAULT_RUNNER_PATH.clone())
            .build()?;

        config.save_to(local_config_path)?;

        pb.println(format!(
            "🎉 {}",
            format!(
                "Successfully upgraded Local Fluvio cluster to {}",
                platform_version
            )
            .bold(),
        ));

        pb.println(format!(
            "Run: {} to start the cluster again",
            "fluvio cluster resume".bold()
        ));
        Ok(())
    }
}

async fn check_all_metadata(pb: &ProgressRenderer, client: &LocalMetadataStorage) -> Result<()> {
    pb.println(format!("📝 {}", "Checking All Metadatas".bold()));
    let _ = client.retrieve_items::<TopicSpec>(&NameSpace::All).await?;
    let _ = client
        .retrieve_items::<PartitionSpec>(&NameSpace::All)
        .await?;
    let _ = client.retrieve_items::<SpuSpec>(&NameSpace::All).await?;
    client
        .retrieve_items::<SmartModuleSpec>(&NameSpace::All)
        .await?;
    let _ = client
        .retrieve_items::<SpuGroupSpec>(&NameSpace::All)
        .await?;
    let _ = client
        .retrieve_items::<TableFormatSpec>(&NameSpace::All)
        .await?;
    let _ = client.retrieve_items::<MirrorSpec>(&NameSpace::All).await?;

    pb.println(format!("✅ {}", "Checked All Metadata".bold()));
    Ok(())
}
