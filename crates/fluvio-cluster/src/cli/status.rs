use clap::Parser;

use fluvio::{Fluvio, FluvioAdmin, ConsumerConfig};
use fluvio::config::ConfigFile;
use fluvio_controlplane_metadata::{spu::SpuSpec, topic::TopicSpec, partition::PartitionSpec};
use fluvio_future::io::StreamExt;

use crate::{cli::ClusterCliError, cli::ClusterTarget};

#[derive(Debug, Parser)]
pub struct StatusOpt {}

impl StatusOpt {
    pub async fn process(self, target: ClusterTarget) -> Result<(), ClusterCliError> {
        let fluvio_config = target.load()?;

        let fluvio = match Fluvio::connect_with_config(&fluvio_config).await {
            Ok(fluvio) => {
                println!("Cluster Running {}", Self::cluster_location_description()?);

                fluvio
            }
            Err(_err) => {
                println!("none");

                return Ok(());
            }
        };

        let admin = FluvioAdmin::connect_with_config(&fluvio_config).await;
        match admin {
            Ok(admin) => {
                if Self::spus_running(&admin).await {
                    Self::check_spus_for_data(&fluvio, &admin).await
                } else {
                    println!("no spus running");
                }
            }
            Err(e) => {
                return Err(e.into());
            }
        };

        Ok(())
    }

    async fn spus_running(admin: &FluvioAdmin) -> bool {
        let filters: Vec<String> = vec![];
        let spus = admin.list::<SpuSpec, _>(filters).await;

        match spus {
            Ok(spus) => spus.iter().any(|spu| spu.status.is_online()),
            Err(_) => false,
        }
    }

    async fn check_spus_for_data(fluvio: &Fluvio, admin: &FluvioAdmin) {
        if !Self::cluster_has_data(fluvio, admin).await {
            println!("no spus running");
        }
    }

    /// Check if any topic in the cluster has data in any partitions.
    async fn cluster_has_data(fluvio: &Fluvio, admin: &FluvioAdmin) -> bool {
        let topics = Self::topics(admin).await;

        for topic in topics {
            let partitions = match Self::num_partitions(admin, &topic).await {
                Ok(partitions) => partitions,
                Err(_) => return false,
            };

            for partition in 0..partitions {
                if let Ok(Some(_record)) = Self::last_record(fluvio, &topic, partition as u32).await
                {
                    return true;
                }
            }
        }

        false
    }

    /// All the topics served by the cluster
    async fn topics(admin: &FluvioAdmin) -> Vec<String> {
        let filters: Vec<String> = vec![];
        let topics = admin.list::<TopicSpec, _>(filters).await;

        match topics {
            Ok(topics) => topics.iter().map(|t| t.name.to_string()).collect(),
            Err(_) => vec![],
        }
    }

    /// Get the number of partiitions for a given topic
    async fn num_partitions(admin: &FluvioAdmin, topic: &str) -> Result<usize, ClusterCliError> {
        let partitions = admin
            .list::<PartitionSpec, _>(vec![topic.to_string()])
            .await;

        Ok(partitions?.len())
    }

    /// Get the last record in a given partition of a given topic
    async fn last_record(
        fluvio: &Fluvio,
        topic: &str,
        partition: u32,
    ) -> Result<Option<String>, ClusterCliError> {
        let consumer = fluvio.partition_consumer(topic, partition).await?;

        let consumer_config = ConsumerConfig::builder().disable_continuous(true).build()?;

        let mut stream = consumer
            .stream_with_config(fluvio::Offset::from_end(1), consumer_config)
            .await?;

        if let Some(Ok(record)) = stream.next().await {
            let string = String::from_utf8_lossy(record.value());
            return Ok(Some(string.to_string()));
        }

        Ok(None)
    }

    fn cluster_location_description() -> Result<String, ClusterCliError> {
        let config = ConfigFile::load_default_or_new()?;

        match config.config().current_profile_name() {
            Some("local") => Ok("locally".to_string()),
            // Cloud cluster
            Some(other) if other.contains("cloud") => Ok("on cloud based k8s".to_string()),
            _ => Ok("on local k8s".to_string()),
        }
    }
}
