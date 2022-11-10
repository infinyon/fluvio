use std::convert::{TryFrom, TryInto};
use std::fmt::{Display, Debug};
use std::io::Error as IoError;
use std::io::ErrorKind;

use fluvio_protocol::{Decoder, Encoder};
use fluvio_protocol::api::{Request, RequestMessage};
use fluvio_future::net::DomainConnector;
use futures_util::{Stream, StreamExt};
use tracing::{debug, trace, instrument};

use fluvio_sc_schema::objects::{
    CommonCreateRequest, DeleteRequest, ObjectApiCreateRequest, ObjectApiDeleteRequest,
    ObjectApiListRequest, ObjectApiListResponse, ObjectApiWatchRequest, Metadata, ListFilter,
    WatchRequest, ObjectApiWatchResponse, WatchResponse,
};
use fluvio_sc_schema::{AdminSpec, DeletableAdminSpec, CreatableAdminSpec};
use fluvio_socket::{SocketError, ClientConfig, VersionedSerialSocket, SerialFrame, MultiplexerSocket};

use crate::{FluvioError, FluvioConfig};
use crate::metadata::objects::{ListResponse, ListRequest};
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
        let client_config =
            ClientConfig::new(&config.endpoint, connector, config.use_spu_local_address);
        let inner_client = client_config.connect().await?;
        debug!(addr = %inner_client.config().addr(), "connected to cluster");

        let (socket, config, versions) = inner_client.split();
        if let Some(watch_version) = versions.lookup_version::<ObjectApiWatchRequest>() {
            let socket = MultiplexerSocket::shared(socket);
            let metadata = MetadataStores::start(socket.clone(), watch_version).await?;
            let versioned_socket = VersionedSerialSocket::new(socket, config, versions);

            Ok(Self {
                socket: versioned_socket,
                metadata,
            })
        } else {
            Err(FluvioError::Other("WatchApi version not found".to_string()))
        }
    }

    #[instrument(skip(self, request))]
    async fn send_receive<R>(&self, request: R) -> Result<R::Response, SocketError>
    where
        R: Request + Send + Sync,
    {
        self.socket.send_receive(request).await
    }

    /// Create new object
    #[instrument(skip(self, name, dry_run, spec))]
    pub async fn create<S>(&self, name: String, dry_run: bool, spec: S) -> Result<(), FluvioError>
    where
        S: CreatableAdminSpec + Sync + Send,
        ObjectApiCreateRequest: From<(CommonCreateRequest, S)>,
    {
        let common_request = CommonCreateRequest {
            name,
            dry_run,
            ..Default::default()
        };

        self.create_with_config(common_request, spec).await
    }

    #[instrument(skip(self, config, spec))]
    pub async fn create_with_config<S>(
        &self,
        config: CommonCreateRequest,
        spec: S,
    ) -> Result<(), FluvioError>
    where
        S: CreatableAdminSpec + Sync + Send,
        ObjectApiCreateRequest: From<(CommonCreateRequest, S)>,
    {
        let create_request: ObjectApiCreateRequest = (config, spec).into();

        debug!("sending create request: {:#?}", create_request);

        self.send_receive(create_request).await?.as_result()?;

        Ok(())
    }

    /// Delete object by key
    /// key is depend on spec, most are string but some allow multiple types
    #[instrument(skip(self, key))]
    pub async fn delete<S, K>(&self, key: K) -> Result<(), FluvioError>
    where
        S: DeletableAdminSpec + Sync + Send,
        K: Into<S::DeleteKey>,
        ObjectApiDeleteRequest: From<DeleteRequest<S>>,
    {
        let delete_request = DeleteRequest::new(key.into());
        let delete_request: ObjectApiDeleteRequest = delete_request.into();

        debug!("sending delete request: {:#?}", delete_request);

        self.send_receive(delete_request).await?.as_result()?;
        Ok(())
    }

    /// return all instance of this spec
    #[instrument(skip(self))]
    pub async fn all<S>(&self) -> Result<Vec<Metadata<S>>, FluvioError>
    where
        S: AdminSpec,
        ObjectApiListRequest: From<ListRequest<S>>,
        ListResponse<S>: TryFrom<ObjectApiListResponse>,
        <ListResponse<S> as TryFrom<ObjectApiListResponse>>::Error: Display,
        S::Status: Encoder + Decoder + Debug,
    {
        self.list_with_params::<S, String>(vec![], false).await
    }

    /// return all instance of this spec by filter
    #[instrument(skip(self, filters))]
    pub async fn list<S, F>(&self, filters: Vec<F>) -> Result<Vec<Metadata<S>>, FluvioError>
    where
        S: AdminSpec,
        ListFilter: From<F>,
        ObjectApiListRequest: From<ListRequest<S>>,
        ListResponse<S>: TryFrom<ObjectApiListResponse>,
        <ListResponse<S> as TryFrom<ObjectApiListResponse>>::Error: Display,
        S::Status: Encoder + Decoder + Debug,
    {
        self.list_with_params(filters, false).await
    }

    #[instrument(skip(self, filters))]
    pub async fn list_with_params<S, F>(
        &self,
        filters: Vec<F>,
        summary: bool,
    ) -> Result<Vec<Metadata<S>>, FluvioError>
    where
        S: AdminSpec,
        ListFilter: From<F>,
        ObjectApiListRequest: From<ListRequest<S>>,
        ListResponse<S>: TryFrom<ObjectApiListResponse>,
        <ListResponse<S> as TryFrom<ObjectApiListResponse>>::Error: Display,
        S::Status: Encoder + Decoder + Debug,
    {
        use std::io::Error as IoError;
        use std::io::ErrorKind;

        let filter_list: Vec<ListFilter> = filters.into_iter().map(Into::into).collect();
        let list_request = ListRequest::new(filter_list, summary);

        let list_request: ObjectApiListRequest = list_request.into();
        let response = self.send_receive(list_request).await?;
        trace!("list response: {:#?}", response);
        response
            .try_into()
            .map_err(|err| IoError::new(ErrorKind::Other, format!("can't convert: {}", err)).into())
            .map(|out: ListResponse<S>| out.inner())
    }

    /// Watch stream of changes for metadata
    /// There is caching, this is just pass through
    #[instrument(skip(self))]
    pub async fn watch<S>(
        &self,
    ) -> Result<impl Stream<Item = Result<WatchResponse<S>, IoError>>, FluvioError>
    where
        S: AdminSpec,
        ObjectApiWatchRequest: From<WatchRequest<S>>,
        S::Status: Encoder + Decoder,
        WatchResponse<S>: TryFrom<ObjectApiWatchResponse>,
        <WatchResponse<S> as TryFrom<ObjectApiWatchResponse>>::Error: Display + Send,
    {
        // only summary for watch
        let watch_request: WatchRequest<S> = WatchRequest::summary();

        let watch_req: ObjectApiWatchRequest = watch_request.into();
        let req_msg = RequestMessage::new_request(watch_req);
        debug!(api_version = req_msg.header.api_version(), obj = %S::LABEL, "create watch stream");
        let inner_socket = self.socket.new_socket();
        let stream = inner_socket.create_stream(req_msg, 10).await?;
        Ok(stream.map(|respons_result| match respons_result {
            Ok(response) => {
                let watch_response: Result<WatchResponse<S>, IoError> =
                    response.try_into().map_err(|err| {
                        IoError::new(ErrorKind::Other, format!("can't convert: {}", err))
                    });
                watch_response
            }
            Err(err) => Err(IoError::new(
                ErrorKind::Other,
                format!("socket error {}", err),
            )),
        }))
    }
}

/// API for streaming cached metadata
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
