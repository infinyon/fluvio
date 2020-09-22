use std::convert::TryFrom;

use tracing::debug;
use kf_socket::AllMultiplexerSocket;


use crate::config::ConfigFile;
use crate::admin::FluvioAdmin;
use crate::TopicProducer;
use crate::PartitionConsumer;
use crate::FluvioError;
use crate::FluvioConfig;
use crate::sync::MetadataStores;
use crate::spu::SpuPool;

use super::*;
use fluvio_future::tls::AllDomainConnector;
use std::convert::TryFrom;

/// An interface for interacting with Fluvio streaming
pub struct Fluvio {
    socket: AllMultiplexerSocket,
    config: ClientConfig,
    versions: Versions,
    spu_pool: SpuPool,
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
        let config_file = ConfigFile::load_default_or_new()?;
        let cluster_config = config_file
            .config()
            .current_cluster()
            .ok_or_else(|| FluvioError::ConfigError("failed to load cluster config".to_string()))?;
        Self::connect_with_config(cluster_config).await
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
        let connector = AllDomainConnector::try_from(config.tls.clone())?;
        let config = ClientConfig::new(&config.addr, connector);
        let inner_client = config.connect().await?;
        debug!("connected to cluster at: {}", inner_client.config().addr());

        let (socket, config, versions) = inner_client.split();
        let mut socket = AllMultiplexerSocket::new(socket);

        let metadata = MetadataStores::new(&mut socket).await?;
        let spu_pool = SpuPool::new(config.clone(), metadata);

        Ok(Self {
            socket,
            config,
            versions,
            spu_pool,
        })
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
    /// # use fluvio::{Fluvio, FluvioError};
    /// # async fn do_produce_to_topic(fluvio: &Fluvio) -> Result<(), FluvioError> {
    /// let producer = fluvio.topic_producer("my-topic").await?;
    /// producer.send_record("Hello, Fluvio!", 0).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn topic_producer<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<TopicProducer, FluvioError> {
        let topic = topic.into();
        debug!(topic = &*topic, "Creating producer");
        Ok(TopicProducer::new(topic, self.spu_pool.clone()))
    }

    /// Creates a new `PartitionConsumer` for the given topic and partition
    ///
    /// Currently, consumers are scoped to both a specific Fluvio topic
    /// _and_ to a particular partition within that topic. That means that
    /// if you have a topic with multiple partitions, then in order to receive
    /// all of the events in all of the partitions, you will need to create
    /// one consumer per partition.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{Fluvio, Offset, FluvioError};
    /// # async fn do_consume_from_partitions(fluvio: &Fluvio) -> Result<(), FluvioError> {
    /// let consumer_one = fluvio.partition_consumer("my-topic", 0).await?;
    /// let consumer_two = fluvio.partition_consumer("my-topic", 1).await?;
    ///
    /// let records_one = consumer_one.fetch(Offset::beginning()).await?;
    /// let records_two = consumer_two.fetch(Offset::beginning()).await?;
    /// # Ok(())
    /// # }
    /// ```
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
            self.spu_pool.clone(),
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
    pub async fn admin(&mut self) -> FluvioAdmin {
        FluvioAdmin::new(self.create_serial_client().await)
    }

    /// create serial connection
    async fn create_serial_client(&mut self) -> VersionedSerialSocket {
        VersionedSerialSocket::new(
            self.socket.create_serial_socket().await,
            self.config.clone(),
            self.versions.clone(),
        )
    }
}
