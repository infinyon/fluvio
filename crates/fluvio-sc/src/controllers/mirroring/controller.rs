use std::time::{Duration, SystemTime};

use tracing::{debug, error, info, instrument};
use anyhow::{anyhow, Result};
use adaptive_backoff::prelude::{
    ExponentialBackoffBuilder, BackoffBuilder, ExponentialBackoff, Backoff,
};

use fluvio::config::TlsPolicy;
use fluvio_socket::{AsyncResponse, ClientConfig, MultiplexerSocket, StreamSocket};
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

const MIRRORING_CONTROLLER_INTERVAL: u64 = 1;

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
        let mut backoff = create_backoff();

        loop {
            if let Err(err) = self.inner_loop(&mut backoff).await {
                error!("error with inner loop: {:#?}", err);
                if let Err(err) = self
                    .update_status(MirrorPairStatus::DetailFailure(err.to_string()))
                    .await
                {
                    error!("error updating status: {:#?}", err);
                }

                let wait = backoff.wait();
                sleep(wait).await;
            }
        }
    }

    #[instrument(skip(self))]
    async fn inner_loop(&self, backoff: &mut ExponentialBackoff) -> Result<()> {
        loop {
            if let Some((home, _)) = self.get_mirror_home_cluster().await {
                debug!("initializing listeners");
                let home_config = self.build_home_client(&home).await?;
                let mut stream = self.request_stream(&home, home_config).await?;

                while let Some(response) = stream.next().await {
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
                                self.sync_topic(&home, topic).await?;
                            }

                            info!("synced topics from home");
                            self.update_status(MirrorPairStatus::Succesful).await?;
                            backoff.reset();
                        }
                        Err(err) => {
                            error!("received error: {:#?}", err);
                            return Err(err.into());
                        }
                    }
                }
            }

            debug!(
                "sleeping {}s before next iteration",
                MIRRORING_CONTROLLER_INTERVAL
            );
            sleep(Duration::from_secs(MIRRORING_CONTROLLER_INTERVAL)).await;
        }
    }

    async fn request_stream(
        &self,
        home: &Home,
        home_config: ClientConfig,
    ) -> Result<AsyncResponse<ObjectMirroringRequest>> {
        let versioned_socket = home_config.connect().await?;
        let (socket, config, versions) = versioned_socket.split();
        info!(endpoint = home.public_endpoint, "connection to home");

        let request = MirrorConnect {
            remote_id: home.remote_id.clone(),
        };
        debug!(request = ?request, "sending connect request");

        let mut stream_socket =
            StreamSocket::new(config, MultiplexerSocket::shared(socket), versions.clone());

        let version = versions
            .lookup_version::<ObjectMirroringRequest>()
            .ok_or(anyhow!("no version found for mirroring request"))?;

        let req = ObjectMirroringRequest::try_encode_from(
            MirroringRemoteClusterRequest { request },
            version,
        )?;

        let stream = stream_socket
            .create_stream_with_version(req, version)
            .await?;

        Ok(stream)
    }

    // Build the home client
    async fn build_home_client(&self, home: &Home) -> Result<ClientConfig> {
        let tlspolicy = option_tlspolicy(home);

        // handling tls
        if let Some(tlspolicy) = tlspolicy {
            match DomainConnector::try_from(tlspolicy.clone()) {
                Ok(connector) => Ok(ClientConfig::new(
                    home.public_endpoint.clone(),
                    connector,
                    false,
                )),
                Err(err) => {
                    error!(
                        "error establishing tls with leader at: <{}> err: {}",
                        home.public_endpoint.clone(),
                        err
                    );
                    Err(err)
                }
            }
        } else {
            Ok(ClientConfig::with_addr(home.public_endpoint.clone()))
        }
    }

    // Get the mirror home
    async fn get_mirror_home_cluster(&self) -> Option<(Home, MirrorStatus)> {
        self.mirrors.store().read().await.values().find_map(|r| {
            match r.spec().mirror_type.clone() {
                MirrorType::Home(h) => Some((h, r.status.clone())),
                _ => None,
            }
        })
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
                        key: topic.spu_key.clone(),
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

    // Update the status of the mirror
    async fn update_status(&self, pair_status: MirrorPairStatus) -> Result<()> {
        let (home, status) = self
            .get_mirror_home_cluster()
            .await
            .ok_or(anyhow!("no home cluster found"))?;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_millis();
        let new_status = MirrorStatus::new(pair_status, ConnectionStatus::Online, now as u64);
        let mut status = status.clone();
        status.merge_from_sc(new_status);
        self.mirrors.update_status(home.id.clone(), status).await?;
        Ok(())
    }
}

fn create_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::default()
        .factor(1.1)
        .min(Duration::from_secs(1))
        .max(Duration::from_secs(30))
        .build()
        .unwrap()
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
