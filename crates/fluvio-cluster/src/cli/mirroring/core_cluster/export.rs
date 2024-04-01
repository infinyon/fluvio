use std::sync::Arc;

use anyhow::{Result, Context};
use clap::Parser;

use k8_types::K8Obj;
use fluvio::config::{TlsPolicy, TlsConfig};
// use fluvio_controlplane::upstream_cluster::{UpstreamClusterSpec, UpstreamTarget, ClientTls};
use fluvio_controlplane_metadata::{
    spu::SpuSpec,
    topic::{MirrorConfig, ReplicaSpec, SourceMirrorConfig, TopicSpec},
    upstream_cluster::{ClientTls, UpstreamClusterSpec, UpstreamTarget},
};
use fluvio_extension_common::{target::ClusterTarget, Terminal};

#[derive(Debug, Parser)]
pub struct ExportOpt {
    /// topic name
    topic: String,
    /// id of mirror cluster
    #[arg(long, short = 'm')]
    mirror: String,
    /// name of the file where we should put the file
    #[arg(long, short = 'f')]
    file: Option<String>,

    /// override endpoint of the main cluster
    #[arg(long, short = 'e')]
    upstream: Option<String>,
}
impl ExportOpt {
    pub async fn execute<T: Terminal>(
        self,
        out: Arc<T>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        let fluvio_config = cluster_target.load()?;
        let flv = fluvio::Fluvio::connect_with_config(&fluvio_config).await?;
        let admin = flv.admin().await;

        let topics = admin.all::<TopicSpec>().await?;
        let topic = topics
            .into_iter()
            .find(|t| t.name == self.topic)
            .context("Topic not found")?;

        // TODO: make configurable or generate!
        let upstream_cluster_id = "upstream".to_string();

        let topic_replica_map = topic.status.replica_map;

        let (source_topic_spec, target_spu_id) = if let ReplicaSpec::Mirror(
            fluvio_controlplane_metadata::topic::MirrorConfig::Target(mirror_config),
        ) = topic.spec.replicas()
        {
            // generate topic spec for source topic

            // count all remote clusters that are pointing to the given mirror cluster
            let partition = mirror_config
                .partitions()
                .iter()
                .position(|rc| rc.remote_cluster == self.mirror);

            if partition.is_none() {
                return Err(anyhow::anyhow!(
                    "Topic {} is not a mirror target for cluster {}",
                    self.topic,
                    self.mirror
                ));
            }

            let partition = partition.unwrap() as u32;
            let target_spu_id = topic_replica_map
                .get(&partition)
                .context("Topic does not have a replica for {partition}")?
                .first()
                .context("Topic does not have any replicas")?;

            let replica: ReplicaSpec =
                ReplicaSpec::Mirror(MirrorConfig::Source(SourceMirrorConfig {
                    target_spus: vec![*target_spu_id; 1],
                    upstream_cluster: upstream_cluster_id.clone(),
                }));

            let mut source_topic: TopicSpec = replica.into();
            if let Some(cleanup_policy) = topic.spec.get_clean_policy() {
                source_topic.set_cleanup_policy(cleanup_policy.clone())
            }

            source_topic.set_compression_type(topic.spec.get_compression_type().clone());

            source_topic.set_deduplication(topic.spec.get_deduplication().cloned());

            if let Some(storage) = topic.spec.get_storage() {
                source_topic.set_storage(storage.clone());
            }
            (source_topic, *target_spu_id)
        } else {
            return Err(anyhow::anyhow!("Topic is not a mirror target"));
        };

        let spu_endpoint = match self.upstream {
            Some(mut endpoint) => {
                let v: Vec<&str> = endpoint.split(':').collect();

                if v.len() > 2 {
                    return Err(anyhow::anyhow!("invalid host:port format {}", endpoint));
                }

                // if only host is provided, assume default port
                if v.len() == 1 {
                    endpoint.push_str(":9010")
                }

                endpoint
            }
            None => admin
                .all::<SpuSpec>()
                .await?
                .into_iter()
                .find(|s| s.spec.id == target_spu_id)
                .context("not found spu endpoint")?
                .spec
                .public_endpoint
                .addr(),
        };

        let topics = vec![K8Obj::new(self.topic, source_topic_spec)];

        let tls = match fluvio_config.tls {
            TlsPolicy::Verified(TlsConfig::Inline(config)) => Some(ClientTls {
                domain: config.domain.clone(),
                ca_cert: config.ca_cert.clone(),
                client_cert: config.cert.clone(),
                client_key: config.key.clone(),
            }),
            // verify that this works as expected
            TlsPolicy::Verified(TlsConfig::Files(file_config)) => Some(ClientTls {
                domain: file_config.domain.clone(),
                ca_cert: std::fs::read_to_string(file_config.ca_cert)?,
                client_cert: std::fs::read_to_string(file_config.cert)?,
                client_key: std::fs::read_to_string(file_config.key)?,
            }),
            _ => None,
        };

        let upstream_clusters = vec![K8Obj::new(
            upstream_cluster_id,
            UpstreamClusterSpec {
                target: UpstreamTarget {
                    endpoint: spu_endpoint,
                    tls,
                },
                source_id: self.mirror,
                ..Default::default()
            },
        )];

        let metadata = fluvio_sc_schema::edge::EdgeMetadataExport::new(topics, upstream_clusters);

        if let Some(filename) = self.file {
            std::fs::write(filename, serde_json::to_string_pretty(&metadata)?)
                .context("Failed to write output file")?;
        } else {
            out.println(&serde_json::to_string_pretty(&metadata)?);
        }

        Ok(())
    }
}
