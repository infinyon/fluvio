use tracing::debug;

use kf_socket::AllMultiplexerSocket;
use dataplane::ReplicaKey;

use crate::admin::FluvioAdmin;
use crate::PartitionProducer;
use crate::PartitionConsumer;
use crate::FluvioError;
use crate::FluvioConfig;
use crate::sync::MetadataStores;
use crate::spu::SpuPool;

use super::*;
use flv_future_aio::net::tls::AllDomainConnector;
use std::convert::TryFrom;
use crate::config::ConfigFile;

/// An interface for interacting with Fluvio streaming
pub struct Fluvio {
    socket: AllMultiplexerSocket,
    config: ClientConfig,
    versions: Versions,
    spu_pool: SpuPool,
}

impl Fluvio {
    /// Creates a new Fluvio client with default configurations
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
        let cluster_config = config_file.config().current_cluster()
            .ok_or(FluvioError::ConfigError(format!("failed to load cluster config")))?;
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
        let connector = AllDomainConnector::try_from(&config.tls)?;
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

    /// create new producer for topic/partition
    pub async fn partition_producer<S: Into<String>>(&mut self, topic: S, partition: i32) -> Result<PartitionProducer, FluvioError> {
        let replica = ReplicaKey::new(topic, partition);
        debug!("creating producer, replica: {}", replica);
        Ok(PartitionProducer::new(replica, self.spu_pool.clone()))
    }

    /// create new consumer for topic/partition
    pub async fn partition_consumer<S: Into<String>>(&mut self, topic: S, partition: i32) -> Result<PartitionConsumer, FluvioError> {
        let replica = ReplicaKey::new(topic, partition);
        debug!("creating consumer, replica: {}", replica);
        Ok(PartitionConsumer::new(replica, self.spu_pool.clone()))
    }

    /// Provides an interface for managing a Fluvio cluster
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{Fluvio, FluvioError};
    /// # async fn do_get_admin(fluvio: &mut Fluvio) -> Result<(), FluvioError> {
    /// let admin = fluvio.admin().await?;
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
