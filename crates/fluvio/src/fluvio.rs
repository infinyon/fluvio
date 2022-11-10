use std::convert::TryFrom;
use std::sync::Arc;

use fluvio_sc_schema::objects::ObjectApiWatchRequest;
use tracing::{debug, info};
use tokio::sync::OnceCell;

use fluvio_socket::{
    ClientConfig, Versions, VersionedSerialSocket, SharedMultiplexerSocket, MultiplexerSocket,
};
use fluvio_future::net::DomainConnector;
use semver::Version;

use crate::admin::FluvioAdmin;
use crate::TopicProducer;
use crate::PartitionConsumer;

use crate::FluvioError;
use crate::FluvioConfig;
use crate::consumer::MultiplePartitionConsumer;
use crate::consumer::PartitionSelectionStrategy;
use crate::metrics::ClientMetrics;
use crate::producer::TopicProducerConfig;
use crate::spu::SpuPool;
use crate::sync::MetadataStores;

/// An interface for interacting with Fluvio streaming
pub struct Fluvio {
    socket: SharedMultiplexerSocket,
    config: Arc<ClientConfig>,
    versions: Versions,
    spu_pool: OnceCell<Arc<SpuPool>>,
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
    /// # async fn do_connect() -> Result<(), FluvioError> {
    /// let fluvio = Fluvio::connect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect() -> Result<Self, FluvioError> {
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
    /// # async fn do_connect_with_config() -> Result<(), FluvioError> {
    /// let config_file = ConfigFile::load_default_or_new()?;
    /// let config = config_file.config().current_cluster().unwrap();
    /// let fluvio = Fluvio::connect_with_config(&config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_config(config: &FluvioConfig) -> Result<Self, FluvioError> {
        let connector = DomainConnector::try_from(config.tls.clone())?;
        info!(
            fluvio_crate_version = env!("CARGO_PKG_VERSION"),
            fluvio_git_hash = env!("GIT_HASH"),
            "Connecting to Fluvio cluster"
        );
        Self::connect_with_connector(connector, config).await
    }

    pub async fn connect_with_connector(
        connector: DomainConnector,
        config: &FluvioConfig,
    ) -> Result<Self, FluvioError> {
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
            Err(FluvioError::Other("WatchApi version not found".to_string()))
        }
    }

    /// lazy get spu pool
    async fn spu_pool(&self) -> Result<Arc<SpuPool>, FluvioError> {
        self.spu_pool
            .get_or_try_init(|| async {
                let metadata =
                    MetadataStores::start(self.socket.clone(), self.watch_version).await?;
                let pool = SpuPool::start(self.config.clone(), metadata);
                Ok(Arc::new(pool?))
            })
            .await
            .map(|pool| pool.clone())
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
    /// # async fn do_produce_to_topic(fluvio: &Fluvio) -> Result<(), FluvioError> {
    /// let producer = fluvio.topic_producer("my-topic").await?;
    /// producer.send(RecordKey::NULL, "Hello, Fluvio!").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn topic_producer<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<TopicProducer, FluvioError> {
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
    /// # async fn do_produce_to_topic(fluvio: &Fluvio) -> Result<(), FluvioError> {
    /// let config = TopicProducerConfigBuilder::default().batch_size(500).build()?;
    /// let producer = fluvio.topic_producer_with_config("my-topic", config).await?;
    /// producer.send(RecordKey::NULL, "Hello, Fluvio!").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn topic_producer_with_config<S: Into<String>>(
        &self,
        topic: S,
        config: TopicProducerConfig,
    ) -> Result<TopicProducer, FluvioError> {
        let topic = topic.into();
        debug!(topic = &*topic, "Creating producer");

        let spu_pool = self.spu_pool().await?;
        if !spu_pool.topic_exists(&topic).await? {
            return Err(FluvioError::TopicNotFound(topic));
        }

        TopicProducer::new(topic, spu_pool, config, self.metric.clone()).await
    }

    /// Creates a new `PartitionConsumer` for the given topic and partition
    ///
    /// If you have a topic with multiple partitions, then in order to receive
    /// all of the events in all of the partitions, use `consumer` instead.
    ///
    ///
    pub async fn partition_consumer<S: Into<String>>(
        &self,
        topic: S,
        partition: i32,
    ) -> Result<PartitionConsumer, FluvioError> {
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
    /// # async fn do_consume_from_partitions(fluvio: &Fluvio) -> Result<(), FluvioError> {
    /// # let consumer = fluvio.consumer(PartitionSelectionStrategy::All("my-topic".to_string())).await?;
    /// # let stream = consumer.stream(Offset::beginning()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn consumer(
        &self,
        strategy: PartitionSelectionStrategy,
    ) -> Result<MultiplePartitionConsumer, FluvioError> {
        Ok(MultiplePartitionConsumer::new(
            strategy,
            self.spu_pool().await?,
            self.metric.clone(),
        ))
    }

    /// Provides an interface for managing a Fluvio cluster
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{Fluvio, FluvioError};
    /// # async fn do_get_admin(fluvio: &mut Fluvio) -> Result<(), FluvioError> {
    /// let admin = fluvio.admin().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn admin(&self) -> FluvioAdmin {
        let socket = self.create_serial_client().await;
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
    async fn create_serial_client(&self) -> VersionedSerialSocket {
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
    use fluvio_future::{
        net::{
            BoxReadConnection, BoxWriteConnection, DomainConnector, TcpDomainConnector,
            ConnectionFd,
        },
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
