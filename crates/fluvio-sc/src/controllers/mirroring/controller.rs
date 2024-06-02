use std::time::{Duration, SystemTime};
use tracing::{debug, error, info, instrument};
use anyhow::{anyhow, Result};

use fluvio::config::TlsPolicy;
use fluvio_socket::{ClientConfig, MultiplexerSocket, StreamSocket};
use futures_util::StreamExt;
use fluvio_future::{net::DomainConnector, task::spawn, timer::sleep};
use fluvio_sc_schema::{
    core::MetadataItem,
    mirror::{ConnectionStatus, Home, MirrorPairStatus, MirrorSpec, MirrorStatus, MirrorType},
    mirroring::ObjectMirroringRequest,
    topic::{MirrorConfig, RemoteMirrorConfig, ReplicaSpec, SpuMirrorConfig, TopicSpec},
    TryEncodableFrom,
};
use fluvio_stream_dispatcher::store::StoreContext;
use fluvio_controlplane_metadata::mirroring::{
    MirrorConnect, MirroringRemoteClusterRequest, MirroringSpecWrapper,
};

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
                let tlspolicy = option_tlspolicy(home);

                // handling tls
                let home_config = if let Some(tlspolicy) = &tlspolicy {
                    match DomainConnector::try_from(tlspolicy.clone()) {
                        Ok(connector) => {
                            ClientConfig::new(home.public_endpoint.clone(), connector, false)
                        }
                        Err(err) => {
                            error!(
                                "error establishing tls with leader at: <{}> err: {}",
                                home.public_endpoint.clone(),
                                err
                            );
                            let now = SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)?
                                .as_millis();
                            let status = MirrorStatus::new(
                                MirrorPairStatus::Failed,
                                ConnectionStatus::Online,
                                now as u64,
                            );
                            self.mirrors.update_status(home.id.clone(), status).await?;
                            return Err(err.into());
                        }
                    }
                } else {
                    ClientConfig::with_addr(home.public_endpoint.clone())
                };

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
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)?
                        .as_millis();
                    match response {
                        Ok(response) => {
                            debug!("received response: {:#?}", response);
                            let request_topics_keys = response
                                .topics
                                .iter()
                                .map(|t| t.key.clone())
                                .collect::<Vec<_>>();

                            self.delete_not_received_topics(request_topics_keys, home.id.clone())
                                .await?;

                            for topic in response.topics.iter() {
                                self.sync_topic(home, topic).await?;
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

    // Delete topics that are not in the response
    async fn delete_not_received_topics(&self, topics: Vec<String>, home_id: String) -> Result<()> {
        let all_topics_keys = self
            .topics
            .store()
            .read()
            .await
            .values()
            .filter_map(|t| match t.spec().replicas() {
                ReplicaSpec::Mirror(MirrorConfig::Remote(r)) => {
                    if r.home_cluster == home_id {
                        Some(t.key.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        for topic_key in all_topics_keys.iter() {
            if !topics.contains(topic_key) {
                info!("deleting topic {}", topic_key);
                self.topics.delete(topic_key.to_string()).await?;
            }
        }

        Ok(())
    }

    // Sync the mirror topic
    async fn sync_topic(&self, home: &Home, topic: &MirroringSpecWrapper<TopicSpec>) -> Result<()> {
        // Create a new replica spec for the topic
        let new_replica: ReplicaSpec =
            ReplicaSpec::Mirror(MirrorConfig::Remote(RemoteMirrorConfig {
                home_spus: vec![
                    SpuMirrorConfig {
                        id: topic.spu_id,
                        endpoint: topic.spu_endpoint.clone(),
                    };
                    1
                ],
                home_cluster: home.id.clone(),
            }));

        // Check if the topic already exists
        // If it does, update the replica spec
        // If it doesn't, create a new topic with the replica spec
        let mut remote_topic = if let Some(t) = self.topics.store().read().await.get(&topic.key) {
            let mut topic_spec = t.spec.clone();
            topic_spec.set_replicas(new_replica.clone());

            if topic_spec == t.spec().clone() {
                debug!("topic {} is already up to date", topic.key);
                return Ok(());
            }

            info!("updating topic {} with new replica", topic.key);
            topic_spec
        } else {
            info!("creating new topic {} with new replica", topic.key);

            let mut topic_spec = topic.spec.clone();
            topic_spec.set_replicas(new_replica);
            topic_spec
        };

        if let Some(cleanup_policy) = topic.spec.get_clean_policy() {
            remote_topic.set_cleanup_policy(cleanup_policy.clone())
        }

        remote_topic.set_compression_type(topic.spec.get_compression_type().clone());

        remote_topic.set_deduplication(topic.spec.get_deduplication().cloned());

        if let Some(storage) = topic.spec.get_storage() {
            remote_topic.set_storage(storage.clone());
        }

        self.topics
            .create_spec(topic.key.clone(), remote_topic)
            .await?;

        Ok(())
    }
}

fn option_tlspolicy(home: &Home) -> Option<TlsPolicy> {
    use fluvio::config::{TlsCerts, TlsConfig};

    let ct = match &home.client_tls {
        Some(ct) => ct,
        _ => {
            return None;
        }
    };

    let certs = TlsCerts {
        domain: ct.domain.clone(),
        key: ct.client_key.clone(),
        cert: ct.client_cert.clone(),
        ca_cert: ct.ca_cert.clone(),
    };
    let tlscfg = TlsConfig::Inline(certs);
    Some(TlsPolicy::from(tlscfg))
}
