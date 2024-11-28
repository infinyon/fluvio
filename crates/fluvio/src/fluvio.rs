use std::convert::TryFrom;
use std::sync::Arc;

use anyhow::{Context, Result};
use semver::Version;
use tokio::sync::OnceCell;
use tracing::{debug, info};

use fluvio_future::net::DomainConnector;
use fluvio_sc_schema::partition::PartitionMirrorConfig;
use fluvio_sc_schema::topic::{MirrorConfig, PartitionMap, ReplicaSpec};
use fluvio_sc_schema::objects::ObjectApiWatchRequest;
use fluvio_types::PartitionId;
use fluvio_socket::{
    ClientConfig, Versions, VersionedSerialSocket, SharedMultiplexerSocket, MultiplexerSocket,
};

use crate::admin::FluvioAdmin;
use crate::error::anyhow_version_error;
use crate::consumer::{
    MultiplePartitionConsumer, PartitionSelectionStrategy, ConsumerStream,
    MultiplePartitionConsumerStream, Record, ConsumerConfigExt, ConsumerOffset,
};
use crate::metrics::ClientMetrics;
use crate::producer::{TopicProducerPool, TopicProducerConfig};
use crate::sync::MetadataStores;
use crate::spu::{SpuPool, SpuSocketPool};
use crate::{TopicProducer, PartitionConsumer, FluvioError, FluvioConfig};

/// An interface for interacting with Fluvio streaming
pub struct Fluvio {
    socket: SharedMultiplexerSocket,
    config: Arc<ClientConfig>,
    versions: Versions,
    spu_pool: OnceCell<Arc<SpuSocketPool>>,
    metadata: MetadataStores,
    watch_version: i16,
    metric: Arc<ClientMetrics>,
}

impl Fluvio {
    /// Creates a new Fluvio client using the current profile from `~/.fluvio/config`
    ///
    /// If there is no current profile or the `~/.fluvio/config` file does not exist,
    /// then this will create a new profile with default settings and set it as
    /// current, then try to connect to the cluster using those settings.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{Fluvio, FluvioError};
    /// # async fn do_connect() -> anyhow::Result<()> {
    /// let fluvio = Fluvio::connect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect() -> Result<Self> {
        let cluster_config = FluvioConfig::load()?;
        Self::connect_with_config(&cluster_config).await
    }

    /// Creates a new Fluvio client with the given configuration
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{Fluvio, FluvioError, FluvioConfig};
    /// use fluvio::config::ConfigFile;
    /// # async fn do_connect_with_config() -> anyhow::Result<()> {
    /// let config_file = ConfigFile::load_default_or_new()?;
    /// let config = config_file.config().current_cluster().unwrap();
    /// let fluvio = Fluvio::connect_with_config(&config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_config(config: &FluvioConfig) -> Result<Self> {
        let connector = DomainConnector::try_from(config.tls.clone())?;
        info!(
            fluvio_crate_version = env!("CARGO_PKG_VERSION"),
            "Connecting to Fluvio cluster"
        );
        Self::connect_with_connector(connector, config).await
    }

    /// Creates a new Fluvio client with the given profile
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{Fluvio, FluvioError, FluvioConfig};
    /// use fluvio::config::ConfigFile;
    /// # async fn do_connect_with_profile_name() -> anyhow::Result<()> {
    /// let fluvio = Fluvio::connect_with_profile("local").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_profile(profile: &str) -> Result<Self> {
        let config = FluvioConfig::load_with_profile(profile)?.context(format!(
            "Failed to load cluster config with profile `{profile}`"
        ))?;
        Self::connect_with_config(&config).await
    }

    /// Creates a new Fluvio client with the given connector and configuration
    pub async fn connect_with_connector(
        connector: DomainConnector,
        config: &FluvioConfig,
    ) -> Result<Self> {
        let mut client_config =
            ClientConfig::new(&config.endpoint, connector, config.use_spu_local_address);
        if let Some(client_id) = &config.client_id {
            client_config.set_client_id(client_id.to_owned());
        }
        let inner_client = client_config.connect().await?;
        debug!("connected to cluster");

        let (socket, config, versions) = inner_client.split();

        // get version for watch
        if let Some(watch_version) = versions.lookup_version::<ObjectApiWatchRequest>() {
            debug!(platform = %versions.platform_version(),"checking platform version");
            check_platform_compatible(versions.platform_version())?;

            let socket = MultiplexerSocket::shared(socket);
            let metadata = MetadataStores::start(socket.clone(), watch_version).await?;

            let spu_pool = OnceCell::new();
            Ok(Self {
                socket,
                config,
                versions,
                spu_pool,
                metadata,
                watch_version,
                metric: Arc::new(ClientMetrics::new()),
            })
        } else {
            let platform_version = versions.platform_version().to_string();
            Err(anyhow_version_error(&platform_version))
        }
    }

    /// lazy get spu pool
    async fn spu_pool(&self) -> Result<Arc<SpuSocketPool>> {
        self.spu_pool
            .get_or_try_init(|| async {
                let metadata =
                    MetadataStores::start(self.socket.clone(), self.watch_version).await?;
                let pool = SpuSocketPool::start(self.config.clone(), metadata);
                Ok(Arc::new(pool?))
            })
            .await
            .cloned()
    }

    /// Creates a new `TopicProducer` for the given topic name
    ///
    /// Currently, producers are scoped to a specific Fluvio topic.
    /// That means when you send events via a producer, you must specify
    /// which partition each event should go to.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{Fluvio, FluvioError, RecordKey};
    /// # async fn do_produce_to_topic(fluvio: &Fluvio) -> anyhow::Result<()> {
    /// let producer = fluvio.topic_producer("my-topic").await?;
    /// producer.send(RecordKey::NULL, "Hello, Fluvio!").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn topic_producer(&self, topic: impl Into<String>) -> Result<TopicProducerPool> {
        self.topic_producer_with_config(topic, Default::default())
            .await
    }

    /// Creates a new `TopicProducer` for the given topic name
    ///
    /// Currently, producers are scoped to a specific Fluvio topic.
    /// That means when you send events via a producer, you must specify
    /// which partition each event should go to.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{Fluvio, FluvioError, RecordKey, TopicProducerConfigBuilder};
    /// # async fn do_produce_to_topic(fluvio: &Fluvio) -> anyhow::Result<()> {
    /// let config = TopicProducerConfigBuilder::default().batch_size(500).build()?;
    /// let producer = fluvio.topic_producer_with_config("my-topic", config).await?;
    /// producer.send(RecordKey::NULL, "Hello, Fluvio!").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn topic_producer_with_config(
        &self,
        topic: impl Into<String>,
        config: TopicProducerConfig,
    ) -> Result<TopicProducerPool> {
        let topic = topic.into();
        debug!(topic = &*topic, "Creating producer");

        let spu_pool = self.spu_pool().await?;
        if !spu_pool.topic_exists(topic.clone()).await? {
            return Err(FluvioError::TopicNotFound(topic).into());
        }

        TopicProducer::new(topic, spu_pool, Arc::new(config), self.metric.clone()).await
    }

    /// Creates a new `PartitionConsumer` for the given topic and partition
    ///
    /// If you have a topic with multiple partitions, then in order to receive
    /// all of the events in all of the partitions, use `consumer` instead.
    ///
    ///
    #[deprecated(since = "0.21.8", note = "use `consumer_with_config()` instead")]
    pub async fn partition_consumer(
        &self,
        topic: impl Into<String>,
        partition: PartitionId,
    ) -> Result<PartitionConsumer> {
        let topic = topic.into();
        debug!(topic = &*topic, "Creating consumer");
        Ok(PartitionConsumer::new(
            topic,
            partition,
            self.spu_pool().await?,
            self.metric.clone(),
        ))
    }

    /// Creates a new `MultiplePartitionConsumer`
    ///
    /// Currently, consumers are scoped to both a specific Fluvio topic
    /// _and_ to a particular partition within that topic. That means that
    /// if you have a topic with multiple partitions, then in order to receive
    /// all of the events in all of the partitions, you will need to create
    /// one consumer per partition.
    ///
    /// Records across different partitions are not guaranteed to be ordered.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{Fluvio, Offset, FluvioError, PartitionSelectionStrategy};
    /// # async fn do_consume_from_partitions(fluvio: &Fluvio) -> anyhow::Result<()> {
    /// # let consumer = fluvio.consumer(PartitionSelectionStrategy::All("my-topic".to_string())).await?;
    /// # let stream = consumer.stream(Offset::beginning()).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[deprecated(since = "0.21.8", note = "use `consumer_with_config()` instead")]
    pub async fn consumer(
        &self,
        strategy: PartitionSelectionStrategy,
    ) -> Result<MultiplePartitionConsumer> {
        Ok(MultiplePartitionConsumer::new(
            strategy,
            self.spu_pool().await?,
            self.metric.clone(),
        ))
    }

    /// Creates a new [ConsumerStream] instance.
    ///
    /// The stream can read data from one topic partition or all partitions. Records across different partitions are not guaranteed to be ordered.
    ///
    /// The [ConsumerStream] provides the offset management capabilities. If configured, it allows
    /// to store consumed offsets in the Fluvio cluster, and use it later to continue reading from
    /// the last seen record.
    ///
    /// If the `offset_consumer` property of `ConsumerConfigExt` is specified, the Fluvio fetches
    /// the offset by id and starts the stream from the next available record. If the offset does not exist, the Fluvio creates it.
    /// To read all existing consumers offsets one could use [`Self::consumer_offsets()`] function, to delete - [`Self::delete_consumer_offset()`] function.
    ///
    /// The Fluvio saves offsets once one called [`ConsumerStream::offset_commit()`] method followed by [`ConsumerStream::offset_flush()`].
    /// There is support for auto-commits if [`crate::consumer::OffsetManagementStrategy::Auto`] is used.
    /// # Example
    /// #### Manually commit offsets
    ///
    /// ```no_run
    /// use fluvio::{
    ///    consumer::{ConsumerConfigExtBuilder, ConsumerStream, OffsetManagementStrategy},
    ///    Fluvio, Offset,
    /// };
    /// use futures_util::StreamExt;
    /// async fn do_consume_with_manual_commits(fluvio: &Fluvio) -> anyhow::Result<()> {
    ///    let mut stream = fluvio
    ///        .consumer_with_config(
    ///            ConsumerConfigExtBuilder::default()
    ///                .topic("my-topic".to_string())
    ///                .offset_consumer("my-consumer".to_string())
    ///                .offset_start(Offset::beginning())
    ///                .offset_strategy(OffsetManagementStrategy::Manual)
    ///                .build()?,
    ///        )
    ///        .await?;
    ///    while let Some(Ok(record)) = stream.next().await {
    ///        println!("{}", String::from_utf8_lossy(record.as_ref()));
    ///        stream.offset_commit()?;
    ///        stream.offset_flush().await?;
    ///    }
    ///    Ok(())
    /// }
    /// ```
    /// #### Auto-commits
    ///
    /// ```no_run
    /// use fluvio::{
    ///    consumer::{ConsumerConfigExtBuilder, ConsumerStream, OffsetManagementStrategy},
    ///    Fluvio, Offset,
    /// };
    /// use futures_util::StreamExt;
    /// async fn do_consume_with_auto_commits(fluvio: &Fluvio) -> anyhow::Result<()> {
    ///    let mut stream = fluvio
    ///        .consumer_with_config(
    ///            ConsumerConfigExtBuilder::default()
    ///                .topic("my-topic".to_string())
    ///                .offset_consumer("my-consumer".to_string())
    ///                .offset_start(Offset::beginning())
    ///                .offset_strategy(OffsetManagementStrategy::Auto)
    ///                .build()?,
    ///        )
    ///        .await?;
    ///    while let Some(Ok(record)) = stream.next().await {
    ///        println!("{}", String::from_utf8_lossy(record.as_ref()));
    ///    }
    ///    Ok(())
    /// }
    /// ```
    pub async fn consumer_with_config(
        &self,
        config: ConsumerConfigExt,
    ) -> Result<
        impl ConsumerStream<Item = std::result::Result<Record, fluvio_protocol::link::ErrorCode>>,
    > {
        let spu_pool = self.spu_pool().await?;
        let topic = &config.topic;
        let topics = spu_pool.metadata.topics();
        let topic_spec = topics
            .lookup_by_key(topic)
            .await?
            .ok_or_else(|| FluvioError::TopicNotFound(topic.to_string()))?
            .spec;

        let mirror_partition = if let Some(ref mirror) = &config.mirror {
            match topic_spec.replicas() {
                ReplicaSpec::Mirror(MirrorConfig::Home(home_mirror_config)) => {
                    let partitions_maps =
                        Vec::<PartitionMap>::from(home_mirror_config.as_partition_maps());
                    partitions_maps.iter().find_map(|p| {
                        if let Some(PartitionMirrorConfig::Home(remote)) = &p.mirror {
                            if remote.remote_cluster == *mirror {
                                return Some(p.id);
                            }
                        }
                        None
                    })
                }
                ReplicaSpec::Mirror(MirrorConfig::Remote(remote_mirror_config)) => {
                    let partitions_maps =
                        Vec::<PartitionMap>::from(remote_mirror_config.as_partition_maps());
                    partitions_maps.iter().find_map(|p| {
                        if let Some(PartitionMirrorConfig::Remote(remote)) = &p.mirror {
                            if remote.home_cluster == *mirror {
                                return Some(p.id);
                            }
                        }
                        None
                    })
                }
                _ => None,
            }
        } else {
            None
        };

        let partitions = if let Some(partition) = mirror_partition {
            vec![partition]
        } else if config.partition.is_empty() {
            (0..topic_spec.partitions()).collect()
        } else {
            config.partition.clone()
        };
        let mut partition_streams = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let consumer =
                PartitionConsumer::new(topic.clone(), partition, spu_pool.clone(), self.metrics());
            partition_streams.push(consumer.consumer_stream_with_config(config.clone()).await?);
        }
        Ok(MultiplePartitionConsumerStream::new(partition_streams))
    }

    /// Returns all consumers offsets that currently available in the cluster.
    pub async fn consumer_offsets(&self) -> Result<Vec<ConsumerOffset>> {
        use fluvio_protocol::{link::ErrorCode, record::ReplicaKey};
        use crate::spu::SpuDirectory;

        let spu_pool = self.spu_pool().await?;
        let consumers_replica_id = ReplicaKey::new(
            fluvio_types::defaults::CONSUMER_STORAGE_TOPIC,
            <PartitionId as Default>::default(),
        );
        let socket = spu_pool.create_serial_socket(&consumers_replica_id).await?;
        let response = socket
            .send_receive(fluvio_spu_schema::server::consumer_offset::FetchConsumerOffsetsRequest)
            .await?;
        if response.error_code != ErrorCode::None {
            anyhow::bail!(
                "fetch consumer offsets failed with: {}",
                response.error_code
            );
        }
        Ok(response
            .consumers
            .into_iter()
            .map(ConsumerOffset::from)
            .collect())
    }

    /// Delete a consumer offset for the given name and the replica.
    pub async fn delete_consumer_offset(
        &self,
        consumer_id: impl Into<String>,
        replica_id: impl Into<fluvio_protocol::record::ReplicaKey>,
    ) -> Result<()> {
        use fluvio_protocol::{link::ErrorCode, record::ReplicaKey};

        use crate::spu::SpuDirectory;

        let spu_pool = self.spu_pool().await?;
        let consumers_replica_id = ReplicaKey::new(
            fluvio_types::defaults::CONSUMER_STORAGE_TOPIC,
            <PartitionId as Default>::default(),
        );
        let socket = spu_pool.create_serial_socket(&consumers_replica_id).await?;
        let response = socket
            .send_receive(
                fluvio_spu_schema::server::consumer_offset::DeleteConsumerOffsetRequest {
                    replica_id: replica_id.into(),
                    consumer_id: consumer_id.into(),
                },
            )
            .await?;
        if response.error_code != ErrorCode::None {
            anyhow::bail!(
                "delete consumer offsets failed with: {}",
                response.error_code
            );
        }
        Ok(())
    }

    /// Provides an interface for managing a Fluvio cluster
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{Fluvio, FluvioError};
    /// # async fn do_get_admin(fluvio: &mut Fluvio) -> anyhow::Result<()> {
    /// let admin = fluvio.admin().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn admin(&self) -> FluvioAdmin {
        let socket = self.create_serial_client();
        let metadata = self.metadata.clone();
        FluvioAdmin::new(socket, metadata)
    }

    /// Reports the Platform Version of the connected cluster.
    ///
    /// The "Platform Version" is the value of the VERSION file when
    /// the cluster components were compiled, and is a [`semver`] value.
    ///
    /// [`semver`]: https://semver.org/
    pub fn platform_version(&self) -> &semver::Version {
        self.versions.platform_version()
    }

    /// create serial connection
    fn create_serial_client(&self) -> VersionedSerialSocket {
        VersionedSerialSocket::new(
            self.socket.clone(),
            self.config.clone(),
            self.versions.clone(),
        )
    }

    pub fn metrics(&self) -> Arc<ClientMetrics> {
        self.metric.clone()
    }
}

/// The remote cluster is compatible with this client if its
/// platform version is greater than this crate's
/// `MINIMUM_PLATFORM_VERSION`.
fn check_platform_compatible(cluster_version: &Version) -> Result<(), FluvioError> {
    let client_minimum_version = Version::parse(crate::MINIMUM_PLATFORM_VERSION)
        .expect("MINIMUM_PLATFORM_VERSION must be semver");

    if *cluster_version < client_minimum_version {
        return Err(FluvioError::MinimumPlatformVersion {
            cluster_version: cluster_version.clone(),
            client_minimum_version,
        });
    }

    Ok(())
}

#[cfg(test)]
#[cfg(target_arch = "wasm32")]
mod wasm_tests {
    use async_trait::async_trait;
    use fluvio_ws_stream_wasm::WsMeta;
    use std::io::Error as IoError;
    use fluvio_future::net::{
        BoxReadConnection, BoxWriteConnection, DomainConnector, TcpDomainConnector, ConnectionFd,
    };
    #[derive(Clone, Default)]
    pub struct FluvioWebsocketConnector {}
    impl FluvioWebsocketConnector {
        pub fn new() -> Self {
            Self {}
        }
    }
    #[async_trait(?Send)]
    impl TcpDomainConnector for FluvioWebsocketConnector {
        async fn connect(
            &self,
            addr: &str,
        ) -> Result<(BoxWriteConnection, BoxReadConnection, ConnectionFd), IoError> {
            let addr = if addr == "localhost:9010" {
                "ws://localhost:3001"
            } else {
                addr
            };

            let (mut _ws, wsstream) = WsMeta::connect(addr, None)
                .await
                .map_err(|e| IoError::new(std::io::ErrorKind::Other, e))?;
            let wsstream_clone = wsstream.clone();
            Ok((
                Box::new(wsstream.into_io()),
                Box::new(wsstream_clone.into_io()),
                String::from(addr),
            ))
        }

        fn new_domain(&self, _domain: String) -> DomainConnector {
            Box::new(self.clone())
        }

        fn domain(&self) -> &str {
            "localhost"
        }
    }
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);
    use wasm_bindgen_test::*;
    use super::*;
    use crate::metadata::topic::TopicSpec;
    use futures_util::stream::StreamExt;

    #[wasm_bindgen_test]
    async fn my_test() {
        let config = FluvioConfig::new("ws://localhost:3000");
        let client =
            Fluvio::connect_with_connector(Box::new(FluvioWebsocketConnector::new()), &config)
                .await;
        assert!(client.is_ok());
        let client = client.unwrap();
        let mut admin = client.admin().await;
        let topic = "wasm-test-produce-consume".to_string();
        /*
        let err = admin.create(topic, false, TopicSpec::default()).await;
        tracing::error!("ERROR: {:?}", err);
        assert!(err.is_ok())
        */
        let producer = client.topic_producer(topic.clone()).await;
        assert!(producer.is_ok());
        let producer = producer.unwrap();
        let send = producer.send("foo", "bar").await;

        let consumer = client.partition_consumer(topic, 0).await;
        assert!(consumer.is_ok());
        let consumer = consumer.unwrap();

        let stream = consumer.stream(crate::Offset::beginning()).await;
        assert!(stream.is_ok());
        let mut stream = stream.unwrap();

        stream.next().await;

        for i in 1..10 {
            let key = format!("key-{}", i);
            let value = format!("value-{}", i);
            let send = producer.send(key.clone(), value.clone()).await;
            assert!(send.is_ok());
            let next = stream.next().await;
            assert!(next.is_some());
            let next = next.unwrap();
            assert!(next.is_ok());
            let next = next.unwrap();
            assert_eq!(
                String::from_utf8_lossy(next.key().unwrap()).to_string(),
                key
            );
            assert_eq!(String::from_utf8_lossy(next.value()).to_string(), value);
        }
    }
}
