use clap::Parser;
use crate::{cli::ClusterCliError};
use crate::cli::ClusterTarget;
use fluvio_future::io::StreamExt;
use fluvio_controlplane_metadata::{spu::SpuSpec, topic::TopicSpec, partition::PartitionSpec};

use fluvio::{Fluvio, FluvioConfig, FluvioAdmin};

#[derive(Debug, Parser)]
pub struct StatusOpt {}

impl StatusOpt {
    /// Testing this method
    ///
    /// get address of sc:
    ///
    /// uncomment this before running `flvd cluster status`
    /// ```
    /// println!("sc addr: {}", fluvio_config.endpoint);
    /// ```
    ///
    /// get address of spus:
    /// ```
    /// $ flvd cluster spu list
    /// ```
    ///
    ///
    ///
    ///
    ///
    pub async fn process(self, target: ClusterTarget) -> Result<(), ClusterCliError> {
        let fluvio_config = target.load().unwrap();
        let fluvio = Fluvio::connect_with_config(&fluvio_config).await;

        let sc_running = match fluvio {
            Ok(_fluvio) => true,
            Err(_) => false,
        };

        let admin = FluvioAdmin::connect_with_config(&fluvio_config).await;

        let (spus_running, cluster_has_data) = match admin {
            Ok(admin) => {
                if Self::spus_running(&admin).await {
                    (true, Self::cluster_has_data(&fluvio_config, &admin).await)
                } else {
                    (false, false)
                }
            },
            Err(_) => (false, false),
        };

        let is_local = fluvio_config.endpoint.contains("localhost")
            || fluvio_config.endpoint.contains("127.0.0.1");
        // println!("sc addr: {}", fluvio_config.endpoint);

        match (sc_running, spus_running, cluster_has_data) {
            (true, true, _) => {
                println!("running {}", if is_local { "locally" } else { "on k8s" });
            }
            (true, false, _) => {
                println!("Fluvio cluster is up, but has no spus");
            }
            (false, true, true) => {
                println!("stopped");
            }
            (false, false, _) => {
                println!("none");
            }
            (false, true, false) => {
                println!("sc not running, spu(s) running but empty");
            }
        }

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

    /// Check if any topic in the cluster has data in any partitions.
    async fn cluster_has_data(config: &FluvioConfig, admin: &FluvioAdmin) -> bool {
        let topics = Self::topics(admin).await;

        for topic in topics {
            let partitions = Self::num_partitions(admin, &topic).await;

            for partition in 0..partitions {
                if let Some(_record) = Self::last_record(config, &topic, partition).await {
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
    async fn num_partitions(admin: &FluvioAdmin, topic: &str) -> i32 {
        let partitions = admin
            .list::<PartitionSpec, _>(vec![topic.to_string()])
            .await;

        partitions.unwrap().len() as i32
    }

    /// Get the last record in a given partition of a given topic
    async fn last_record(
        fluvio_config: &FluvioConfig,
        topic: &str,
        partition: i32,
    ) -> Option<String> {
        let fluvio = Fluvio::connect_with_config(fluvio_config).await.unwrap();
        let consumer = fluvio.partition_consumer(topic, partition).await.unwrap();

        let mut stream = consumer.stream(fluvio::Offset::from_end(1)).await.unwrap();

        if let Some(Ok(record)) = stream.next().await {
            let string = String::from_utf8_lossy(record.value());
            return Some(string.to_string());
        }

        None
    }
}
