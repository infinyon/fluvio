use std::convert::TryFrom;
use std::sync::Arc;

use tracing::debug;
use once_cell::sync::OnceCell;

use fluvio_socket::{AllMultiplexerSocket, SharedAllMultiplexerSocket};
use fluvio_future::task::run_block_on;
use fluvio_future::native_tls::AllDomainConnector;

use crate::config::ConfigFile;
use crate::admin::FluvioAdmin;
use crate::TopicProducer;
use crate::PartitionConsumer;
use crate::FluvioError;
use crate::FluvioConfig;
use crate::spu::SpuPool;

use super::*;

/// An interface for interacting with Fluvio streaming
pub struct Fluvio {
    socket: SharedAllMultiplexerSocket,
    config: ClientConfig,
    versions: Versions,
    spu_pool: OnceCell<Arc<SpuPool>>,
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
        let cluster_config = config_file.config().current_cluster()?;
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
        let connector = Arc::new(AllDomainConnector::try_from(config.tls.clone())?);
        let config = ClientConfig::new(&config.addr, connector);
        let inner_client = config.connect().await?;
        debug!("connected to cluster at: {}", inner_client.config().addr());

        let (socket, config, versions) = inner_client.split();
        let socket = AllMultiplexerSocket::shared(socket);

        let spu_pool = OnceCell::new();
        Ok(Self {
            socket,
            config,
            versions,
            spu_pool,
        })
    }

    /// lazy get spu pool
    fn spu_pool(&self) -> Result<Arc<SpuPool>, FluvioError> {
        self.spu_pool
            .get_or_try_init(|| -> Result<Arc<SpuPool>, FluvioError> {
                let pool = run_block_on(SpuPool::start(self.config.clone(), self.socket.clone()));
                Ok(Arc::new(pool?))
            })
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
        Ok(TopicProducer::new(topic, self.spu_pool()?))
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
        Ok(PartitionConsumer::new(topic, partition, self.spu_pool()?))
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
        FluvioAdmin::new(self.create_serial_client().await)
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
}
