use clap::Parser;
use colored::Colorize;
use fluvio::{Fluvio, FluvioAdmin, FluvioConfig};
use fluvio::config::ConfigFile;
use fluvio_controlplane_metadata::{spu::SpuSpec, topic::TopicSpec};
use fluvio_sc_schema::objects::Metadata;
use tracing::debug;

use crate::CheckStatus;
use crate::check::{ActiveKubernetesCluster, ClusterCheck};
use crate::render::ProgressRenderer;
use crate::{cli::ClusterCliError, cli::ClusterTarget};
use crate::progress::ProgressBarFactory;

#[derive(Debug, Parser)]
pub struct StatusOpt {}

macro_rules! pad_format {
    ( $e:expr ) => {
        format!("{:>3} {}", "", $e)
    };
}

impl StatusOpt {
    pub async fn process(self, target: ClusterTarget) -> Result<(), ClusterCliError> {
        let pb_factory = ProgressBarFactory::new(false);

        let pb = match pb_factory.create() {
            Ok(pb) => pb,
            Err(_) => {
                return Err(ClusterCliError::Other(
                    "Failed to create progress bar".to_string(),
                ))
            }
        };

        let fluvio_config = target.load()?;
        let config_file = ConfigFile::load_default_or_new()?;

        pb_factory.println(format!(
            "📝 Running cluster status checks with profile {}",
            Self::profile_name(&config_file).italic()
        ));

        Self::check_k8s_cluster(&pb).await?;
        Self::check_sc(&pb, &fluvio_config, &config_file).await?;
        Self::check_spus(&pb, &fluvio_config).await?;
        Self::check_topics(&pb, &fluvio_config).await?;

        pb.finish_and_clear();

        Ok(())
    }

    async fn check_k8s_cluster(pb: &ProgressRenderer) -> Result<(), ClusterCliError> {
        let k8s_cluster_check = Box::new(ActiveKubernetesCluster);

        pb.set_message(pad_format!(format!(
            "{} Checking {}",
            "📝".bold(),
            k8s_cluster_check.label()
        )));

        match k8s_cluster_check.perform_check(&pb).await? {
            CheckStatus::Pass(status) => {
                pb.println(pad_format!(format!("{} {}", "✅".bold(), status)));
                return Ok(());
            }
            CheckStatus::Unrecoverable(err) => {
                debug!("failed: {}", err);

                pb.println(pad_format!(format!(
                    "{} Check {} failed {}",
                    "❌",
                    k8s_cluster_check.label().italic(),
                    err.to_string().red()
                )));

                return Err(ClusterCliError::Other(err.to_string()));
            }
            _ => {
                return Err(ClusterCliError::Other(
                    "Should not be reachable".to_string(),
                ))
            }
        }
    }

    async fn check_sc(
        pb: &ProgressRenderer,
        fluvio_config: &FluvioConfig,
        config_file: &ConfigFile,
    ) -> Result<(), ClusterCliError> {
        pb.set_message(pad_format!(format!("{} Checking {}", "📝".bold(), "SC")));

        match Fluvio::connect_with_config(&fluvio_config).await {
            Ok(_fluvio) => {
                pb.println(pad_format!(format!("{} SC is ok", "✅".bold())));

                Ok(())
            }
            Err(err) => {
                pb.println(pad_format!(format!(
                    "{} Unable to reach cluster on profile {}, error: {}",
                    "❌",
                    Self::profile_name(config_file).italic(),
                    err.to_string().red()
                )));

                Err(ClusterCliError::Other(err.to_string()))
            }
        }
    }

    async fn check_spus(
        pb: &ProgressRenderer,
        fluvio_config: &FluvioConfig,
    ) -> Result<(), ClusterCliError> {
        pb.set_message(pad_format!(format!("{} Checking {}", "📝".bold(), "SPUs")));

        match FluvioAdmin::connect_with_config(&fluvio_config).await {
            Ok(admin) => {
                let filters: Vec<String> = vec![];
                let spus = admin.list::<SpuSpec, _>(filters).await?;
                let spu_count = spus.len();
                let online_spu_count = spus.iter().filter(|spu| spu.status.is_online()).count();

                if online_spu_count == 0 {
                    pb.println(pad_format!(format!("{} No SPUs are online", "❌".red())));

                    Err(ClusterCliError::Other("No SPUs are online".to_string()))
                } else if online_spu_count < spu_count {
                    pb.println(pad_format!(format!(
                        "{} ({}/{}) SPUs are online",
                        "🟡".yellow(),
                        online_spu_count,
                        spu_count
                    )));

                    Ok(())
                } else {
                    pb.println(pad_format!(format!(
                        "{} ({}/{}) SPUs are online",
                        "✅".bold(),
                        spu_count,
                        spu_count
                    )));

                    Ok(())
                }
            }
            Err(e) => {
                pb.println(pad_format!(format!(
                    "{} Unable to connect to SPUs: {}",
                    "❌".bold(),
                    e.to_string().red()
                )));

                Err(ClusterCliError::ClientError(e))
            }
        }
    }

    fn profile_name(config_file: &ConfigFile) -> String {
        config_file
            .config()
            .current_profile_name()
            .unwrap()
            .to_string()
    }

    async fn check_topics(
        pb: &ProgressRenderer,
        fluvio_config: &FluvioConfig,
    ) -> Result<(), ClusterCliError> {
        pb.set_message(pad_format!(format!("{} Checking {}", "📝".bold(), "Topics")));

        match FluvioAdmin::connect_with_config(&fluvio_config).await {
            Ok(admin) => {
                let topics = Self::topics(&admin).await?;

                if topics.len() == 0 {
                    pb.println(pad_format!(format!("{} No topics present", "🟡".yellow(),)));

                    return Ok(());
                }

                pb.println(pad_format!(format!(
                    "{} {} topics using {}",
                    "✅".bold(),
                    topics.len(),
                    Self::human_readable_size(Self::total_usage_bytes(topics))
                )));

                Ok(())
            }
            Err(e) => {
                pb.println(pad_format!(format!(
                    "{} Unable to retrieve topics: {}",
                    "❌".bold(),
                    e.to_string().red()
                )));

                Err(ClusterCliError::Other(e.to_string()))
            }
        }
    }

    fn total_usage_bytes(topics: Vec<Metadata<TopicSpec>>) -> u64 {
        topics.iter().map(|topic: &Metadata<TopicSpec>| {
            let replications = topic.spec.replication_factor().unwrap() as u64;
            let partitions = topic.spec.partitions() as u64;

            let mut partition_size = 0;
            if let Some(storage) = topic.spec.get_storage() {
                partition_size = storage.max_partition_size.unwrap();
            }

            replications * partitions * partition_size
        }).sum()
    }

    fn human_readable_size(size: u64) -> String {
        if size < 1024 {
            "0 KB".to_string()
        } else if size < 1024 * 1024 {
            format!("{} KB", size / 1024)
        } else if size < 1024 * 1024 * 1024 {
            format!("{} MB", size / (1024 * 1024))
        } else {
            format!("{} GB", size / (1024 * 1024 * 1024))
        }
    }

    async fn topics(admin: &FluvioAdmin) -> Result<Vec<Metadata<TopicSpec>>, ClusterCliError> {
        let filters: Vec<String> = vec![];

        match admin.list::<TopicSpec, _>(filters).await {
            Ok(topics) => Ok(topics),
            Err(e) => Err(ClusterCliError::Other(e.to_string())),
        }
    }
}
