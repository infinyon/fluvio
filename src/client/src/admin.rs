use std::convert::{TryFrom, TryInto};
use std::fmt::Display;

use fluvio_future::net::DomainConnector;
use tracing::{debug, instrument};
use dataplane::core::Encoder;
use dataplane::core::Decoder;
use fluvio_sc_schema::objects::{Metadata, AllCreatableSpec};
use fluvio_sc_schema::AdminRequest;
use fluvio_socket::SocketError;
use fluvio_socket::MultiplexerSocket;

use crate::sockets::{ClientConfig, VersionedSerialSocket, SerialFrame};
use crate::{FluvioError, FluvioConfig};
use crate::metadata::objects::{ListResponse, ListSpec, DeleteSpec, CreateRequest};
use crate::config::ConfigFile;
use crate::sync::MetadataStores;

/// An interface for managing a Fluvio cluster
///
/// Most applications will not require administrator functionality. The
/// `FluvioAdmin` interface is used to create, edit, and manage Topics
/// and other operational items. Think of the difference between regular
/// clients of a Database and its administrators. Regular clients may be
/// applications which are reading and writing data to and from tables
/// that exist in the database. Database administrators would be the
/// ones actually creating, editing, or deleting tables. The same thing
/// goes for Fluvio administrators.
///
/// If you _are_ writing an application whose purpose is to manage a
/// Fluvio cluster for you, you can gain access to the `FluvioAdmin`
/// client via the regular [`Fluvio`] client, or through the [`connect`]
/// or [`connect_with_config`] functions.
///
/// # Example
///
/// Note that this may fail if you are not authorized as a Fluvio
/// administrator for the cluster you are connected to.
///
/// ```no_run
/// # use fluvio::{Fluvio, FluvioError};
/// # async fn do_get_admin(fluvio: &mut Fluvio) -> Result<(), FluvioError> {
/// let admin = fluvio.admin().await;
/// # Ok(())
/// # }
/// ```
///
/// [`Fluvio`]: ./struct.Fluvio.html
/// [`connect`]: ./struct.FluvioAdmin.html#method.connect
/// [`connect_with_config`]: ./struct.FluvioAdmin.html#method.connect_with_config
pub struct FluvioAdmin {
    socket: VersionedSerialSocket,
    #[allow(dead_code)]
    metadata: MetadataStores,
}

impl FluvioAdmin {
    pub(crate) fn new(socket: VersionedSerialSocket, metadata: MetadataStores) -> Self {
        Self { socket, metadata }
    }

    /// Creates a new admin connection using the current profile from `~/.fluvio/config`
    ///
    /// This will attempt to read a Fluvio cluster configuration from
    /// your `~/.fluvio/config` file, or create one with default settings
    /// if you don't have one. If you want to specify a configuration,
    /// see [`connect_with_config`] instead.
    ///
    /// The admin interface requires you to have administrator privileges
    /// on the cluster which you are connecting to. If you don't have the
    /// appropriate privileges, this connection will fail.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{FluvioAdmin, FluvioError};
    /// # async fn do_connect() -> Result<(), FluvioError> {
    /// let admin = FluvioAdmin::connect().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`connect_with_config`]: ./struct.FluvioAdmin.html#method.connect_with_config
    #[instrument]
    pub async fn connect() -> Result<Self, FluvioError> {
        let config_file = ConfigFile::load_default_or_new()?;
        let cluster_config = config_file.config().current_cluster()?;
        Self::connect_with_config(cluster_config).await
    }

    /// Creates a new admin connection using custom configurations
    ///
    /// The admin interface requires you to have administrator privileges
    /// on the cluster which you are connecting to. If you don't have the
    /// appropriate privileges, this connection will fail.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{FluvioAdmin, FluvioError};
    /// use fluvio::config::ConfigFile;
    /// #  async fn do_connect_with_config() -> Result<(), FluvioError> {
    /// let config_file = ConfigFile::load_default_or_new()?;
    /// let fluvio_config = config_file.config().current_cluster().unwrap();
    /// let admin = FluvioAdmin::connect_with_config(fluvio_config).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(config))]
    pub async fn connect_with_config(config: &FluvioConfig) -> Result<Self, FluvioError> {
        let connector = DomainConnector::try_from(config.tls.clone())?;
        let config = ClientConfig::new(&config.endpoint, connector);
        let inner_client = config.connect().await?;
        debug!("connected to cluster at: {}", inner_client.config().addr());

        let (socket, config, versions) = inner_client.split();
        let socket = MultiplexerSocket::shared(socket);
        let metadata = MetadataStores::start(socket.clone()).await?;
        let versioned_socket = VersionedSerialSocket::new(socket, config, versions);

        Ok(Self {
            socket: versioned_socket,
            metadata,
        })
    }

    #[instrument(skip(self, request))]
    async fn send_receive<R>(&self, request: R) -> Result<R::Response, SocketError>
    where
        R: AdminRequest + Send + Sync,
    {
        self.socket.send_receive(request).await
    }

    /// create new object
    #[instrument(skip(self, name, dry_run, spec))]
    pub async fn create<S>(&self, name: String, dry_run: bool, spec: S) -> Result<(), FluvioError>
    where
        S: Into<AllCreatableSpec>,
    {
        let create_request = CreateRequest {
            name,
            dry_run,
            spec: spec.into(),
        };

        self.send_receive(create_request).await?.as_result()?;

        Ok(())
    }

    /// delete object by key
    /// key is depend on spec, most are string but some allow multiple types
    #[instrument(skip(self, key))]
    pub async fn delete<S, K>(&self, key: K) -> Result<(), FluvioError>
    where
        S: DeleteSpec,
        K: Into<S::DeleteKey>,
    {
        let delete_request = S::into_request(key);
        self.send_receive(delete_request).await?.as_result()?;
        Ok(())
    }

    #[instrument(skip(self, filters))]
    pub async fn list<S, F>(&self, filters: F) -> Result<Vec<Metadata<S>>, FluvioError>
    where
        S: ListSpec + Encoder + Decoder,
        S::Status: Encoder + Decoder,
        F: Into<Vec<S::Filter>>,
        ListResponse: TryInto<Vec<Metadata<S>>>,
        <ListResponse as TryInto<Vec<Metadata<S>>>>::Error: Display,
    {
        use std::io::Error;
        use std::io::ErrorKind;

        let list_request = S::into_list_request(filters.into());

        let response = self.send_receive(list_request).await?;

        response
            .try_into()
            .map_err(|err| Error::new(ErrorKind::Other, format!("can't convert: {}", err)).into())
    }
}

#[cfg(feature = "unstable")]
mod unstable {
    use super::*;
    use futures_util::Stream;
    use crate::sync::AlwaysNewContext;
    use crate::metadata::topic::TopicSpec;
    use crate::metadata::partition::PartitionSpec;
    use crate::metadata::spu::SpuSpec;
    use crate::metadata::store::MetadataChanges;

    impl FluvioAdmin {
        /// Create a stream that yields updates to Topic metadata
        pub fn watch_topics(
            &self,
        ) -> impl Stream<Item = MetadataChanges<TopicSpec, AlwaysNewContext>> {
            self.metadata.topics().watch()
        }

        /// Create a stream that yields updates to Partition metadata
        pub fn watch_partitions(
            &self,
        ) -> impl Stream<Item = MetadataChanges<PartitionSpec, AlwaysNewContext>> {
            self.metadata.partitions().watch()
        }

        /// Create a stream that yields updates to SPU metadata
        pub fn watch_spus(&self) -> impl Stream<Item = MetadataChanges<SpuSpec, AlwaysNewContext>> {
            self.metadata.spus().watch()
        }
    }
}
