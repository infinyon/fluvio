use std::time::{Duration, SystemTime};
use tracing::{debug, error, info, instrument};
use anyhow::{anyhow, Context, Result};

use fluvio_socket::{ClientConfig, MultiplexerSocket, StreamSocket};
use futures_util::StreamExt;
use fluvio_future::{task::spawn, timer::sleep};
use fluvio_sc_schema::{
    core::MetadataItem,
    mirror::{ConnectionStatus, MirrorPairStatus, MirrorSpec, MirrorStatus, MirrorType},
    mirroring::ObjectMirroringRequest,
    topic::{MirrorConfig, RemoteMirrorConfig, ReplicaSpec, TopicSpec},
    TryEncodableFrom,
};
use fluvio_stream_dispatcher::store::StoreContext;
use fluvio_controlplane_metadata::mirroring::{MirrorConnect, MirroringRemoteClusterRequest};

use crate::core::SharedContext;

const MIRRORING_CONTROLLER_INTERVAL: u64 = 5;
const MIRRORING_CONTROLLER_RETRY_INTERVAL: u64 = 10;

// This is the main controller for the mirroring feature.
// Remote Clusters will connect to the home clusters.
pub struct RemoteMirrorController<C: MetadataItem> {
    mirrors: StoreContext<MirrorSpec, C>,
    topics: StoreContext<TopicSpec, C>,
}

impl<C: MetadataItem> RemoteMirrorController<C> {
    pub fn start(ctx: SharedContext<C>) {
        let controller = Self {
            mirrors: ctx.mirrors().clone(),
            topics: ctx.topics().clone(),
        };

        info!("starting mirroring controller");
        spawn(controller.dispatch_loop());
    }

    #[instrument(skip(self), name = "MirroringControllerLoop")]
    async fn dispatch_loop(self) {
        info!("started");
        loop {
            if let Err(err) = self.inner_loop().await {
                error!("error with inner loop: {:#?}", err);
                debug!(
                    "sleeping {} seconds try again",
                    MIRRORING_CONTROLLER_RETRY_INTERVAL
                );
                sleep(Duration::from_secs(MIRRORING_CONTROLLER_RETRY_INTERVAL)).await;
            }
        }
    }

    #[instrument(skip(self))]
    async fn inner_loop(&self) -> Result<()> {
        debug!("initializing listeners");

        loop {
            let home_mirrors = self
                .mirrors
                .store()
                .read()
                .await
                .values()
                .filter_map(|r| match r.spec().mirror_type.clone() {
                    MirrorType::Home(h) => Some(h),
                    _ => None,
                })
                .collect::<Vec<_>>();

            if let Some(home) = home_mirrors.first() {
                //send to home cluster the connect request
                //TODO: handle TLS

                let home_config = ClientConfig::with_addr(home.public_endpoint.clone());
                let versioned_socket = home_config.connect().await?;
                let (socket, config, versions) = versioned_socket.split();
                info!("connecting to home: {}", home.public_endpoint);

                let request = MirrorConnect {
                    remote_id: home.remote_id.clone(),
                };
                debug!("sending connect request: {:#?}", request);

                let mut stream_socket =
                    StreamSocket::new(config, MultiplexerSocket::shared(socket), versions.clone());

                let version = versions
                    .lookup_version::<ObjectMirroringRequest>()
                    .ok_or(anyhow!("no version found for mirroring request"))?;

                let req = ObjectMirroringRequest::try_encode_from(
                    MirroringRemoteClusterRequest { request },
                    version,
                )?;

                let mut stream = stream_socket
                    .create_stream_with_version(req, version)
                    .await?;

                while let Some(response) = stream.next().await {
                    // Change status to connected
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)?
                        .as_millis();
                    match response {
                        Ok(response) => {
                            debug!("received response: {:#?}", response);
                            for topic in response.topics.iter() {
                                // find home spu id
                                let remote_topic_spec =
                                    if let ReplicaSpec::Mirror(MirrorConfig::Home(mirror_config)) =
                                        topic.spec.replicas()
                                    {
                                        // generate topic spec for remote topic
                                        // count all remote clusters that are pointing to the given mirror cluster
                                        let partition_id = mirror_config
                                            .partitions()
                                            .iter()
                                            .position(|rc| rc.remote_cluster == home.remote_id)
                                            .context(format!(
                                                "Topic {} is not a mirror home for cluster {}",
                                                topic.key, home.remote_id
                                            ))?
                                            as u32;

                                        let home_spu_id = topic
                                            .replica_map
                                            .get(&partition_id)
                                            .context(
                                                "Topic does not have a replica for {partition_id}",
                                            )?
                                            .first()
                                            .context("Topic does not have any replicas")?;

                                        let new_replica: ReplicaSpec = ReplicaSpec::Mirror(
                                            MirrorConfig::Remote(RemoteMirrorConfig {
                                                home_spus: vec![*home_spu_id; 1],
                                                home_cluster: home.id.clone(),
                                            }),
                                        );

                                        // Check if the topic already exists
                                        let mut remote_topic = if let Some(t) =
                                            self.topics.store().read().await.get(&topic.key)
                                        {
                                            let mut topic_spec = t.spec.clone();
                                            topic_spec.set_replicas(new_replica.clone());

                                            if topic_spec == t.spec().clone() {
                                                debug!("topic {} is already up to date", topic.key);
                                                continue;
                                            }

                                            info!("updating topic {} with new replica", topic.key);
                                            topic_spec
                                        } else {
                                            info!(
                                                "creating new topic {} with new replica",
                                                topic.key
                                            );

                                            let mut topic_spec = topic.spec.clone();
                                            topic_spec.set_replicas(new_replica);
                                            topic_spec
                                        };

                                        if let Some(cleanup_policy) = topic.spec.get_clean_policy()
                                        {
                                            remote_topic.set_cleanup_policy(cleanup_policy.clone())
                                        }

                                        remote_topic.set_compression_type(
                                            topic.spec.get_compression_type().clone(),
                                        );

                                        remote_topic.set_deduplication(
                                            topic.spec.get_deduplication().cloned(),
                                        );

                                        if let Some(storage) = topic.spec.get_storage() {
                                            remote_topic.set_storage(storage.clone());
                                        }
                                        remote_topic
                                    } else {
                                        return Err(anyhow::anyhow!("Topic is not a mirror home"));
                                    };

                                self.topics
                                    .create_spec(topic.key.clone(), remote_topic_spec)
                                    .await?;
                            }

                            let status = MirrorStatus::new(
                                MirrorPairStatus::Succesful,
                                ConnectionStatus::Online,
                                now as u64,
                            );
                            self.mirrors.update_status(home.id.clone(), status).await?;
                        }
                        Err(err) => {
                            debug!("received error: {:#?}", err);
                            let status = MirrorStatus::new(
                                MirrorPairStatus::Failed,
                                ConnectionStatus::Online,
                                now as u64,
                            );
                            self.mirrors.update_status(home.id.clone(), status).await?;

                            return Err(err.into());
                        }
                    }
                }
            }

            debug!(
                "sleeping {} seconds before next iteration",
                MIRRORING_CONTROLLER_INTERVAL
            );
            sleep(Duration::from_secs(MIRRORING_CONTROLLER_INTERVAL)).await;
        }
    }
}
