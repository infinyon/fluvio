use std::convert::TryFrom;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::io::ErrorKind;

use futures_util::{Stream, StreamExt};
use tracing::{debug, trace, instrument};
use anyhow::{Result, anyhow};

use fluvio_sc_schema::objects::ObjectApiUpdateRequest;
use fluvio_sc_schema::objects::UpdateRequest;
use fluvio_sc_schema::UpdatableAdminSpec;
use fluvio_protocol::{Decoder, Encoder};
use fluvio_protocol::api::{Request, RequestMessage};
use fluvio_future::net::DomainConnector;
use fluvio_sc_schema::objects::{
    DeleteRequest, ObjectApiCreateRequest, ObjectApiDeleteRequest, ObjectApiListRequest,
    ObjectApiWatchRequest, Metadata, ListFilter, WatchRequest, WatchResponse, CreateRequest,
    CommonCreateRequest,
};
use fluvio_sc_schema::{AdminSpec, DeletableAdminSpec, CreatableAdminSpec, TryEncodableFrom};
use fluvio_socket::{ClientConfig, VersionedSerialSocket, SerialFrame, MultiplexerSocket};

use crate::FluvioConfig;
use crate::config::ConfigFile;
use crate::error::anyhow_version_error;
use crate::metadata::objects::{ListResponse, ListRequest};
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
    /// # use fluvio::FluvioAdmin;
    /// # async fn do_connect() -> anyhow::Result<()> {
    /// let admin = FluvioAdmin::connect().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`connect_with_config`]: ./struct.FluvioAdmin.html#method.connect_with_config
    #[instrument]
    pub async fn connect() -> Result<Self> {
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
    /// # use fluvio::FluvioAdmin;
    /// use fluvio::config::ConfigFile;
    /// #  async fn do_connect_with_config() -> anyhow::Result<()> {
    /// let config_file = ConfigFile::load_default_or_new()?;
    /// let fluvio_config = config_file.config().current_cluster().unwrap();
    /// let admin = FluvioAdmin::connect_with_config(fluvio_config).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(config))]
    pub async fn connect_with_config(config: &FluvioConfig) -> Result<Self> {
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
            let platform_version = versions.platform_version().to_string();
            Err(anyhow_version_error(&platform_version))
        }
    }

    #[instrument(skip(self, request))]
    async fn send_receive_admin<R, I>(&self, request: I) -> Result<R::Response>
    where
        R: Request + Send + Sync,
        R: TryEncodableFrom<I>,
    {
        let version = self
            .socket
            .lookup_version::<R>()
            .ok_or(anyhow!("no version found for: {}", R::API_KEY))?;
        let request = R::try_encode_from(request, version)?;
        let req_msg = self.socket.new_request(request, Some(version));
        self.socket
            .send_and_receive(req_msg)
            .await
            .map_err(|err| err.into())
    }

    /// Create new object
    #[instrument(skip(self, name, dry_run, spec))]
    pub async fn create<S>(&self, name: String, dry_run: bool, spec: S) -> Result<()>
    where
        S: CreatableAdminSpec + Sync + Send,
    {
        let common_request = CommonCreateRequest {
            name,
            dry_run,
            ..Default::default()
        };

        self.create_with_config(common_request, spec).await
    }

    #[instrument(skip(self, config, spec))]
    pub async fn create_with_config<S>(&self, config: CommonCreateRequest, spec: S) -> Result<()>
    where
        S: CreatableAdminSpec + Sync + Send,
    {
        let create_request = CreateRequest::new(config, spec);
        debug!("sending create request: {:#?}", create_request);

        self.send_receive_admin::<ObjectApiCreateRequest, _>(create_request)
            .await?
            .as_result()?;

        Ok(())
    }

    /// Delete object by key
    /// key is dependent on spec, most are string but some allow multiple types
    ///
    /// For example, to delete a topic:
    ///
    /// ```edition2021
    /// use fluvio::Fluvio;
    /// use fluvio::metadata::topic::TopicSpec;
    ///
    /// async fn delete_topic(name: String) -> anyhow::Result<()> {
    ///     let fluvio = Fluvio::connect().await?;
    ///     let admin = fluvio.admin().await;
    ///     admin.delete::<TopicSpec>(name).await?;
    ///     Ok(())
    /// }
    /// ```
    #[instrument(skip(self, key))]
    pub async fn delete<S>(&self, key: impl Into<S::DeleteKey>) -> Result<()>
    where
        S: DeletableAdminSpec + Sync + Send,
    {
        let delete_request: DeleteRequest<S> = DeleteRequest::new(key.into());
        debug!("sending delete request: {:#?}", delete_request);

        self.send_receive_admin::<ObjectApiDeleteRequest, _>(delete_request)
            .await?
            .as_result()?;
        Ok(())
    }

    /// Forcibly delete object by key
    /// key is dependent on spec, most are string but some allow multiple types.
    ///
    /// This method allows to delete objects marked as 'system'.
    ///
    /// For example, to delete a system topic:
    ///
    /// ```edition2021
    /// use fluvio::Fluvio;
    /// use fluvio::metadata::topic::TopicSpec;
    ///
    /// async fn delete_system_topic(name: String) -> anyhow::Result<()> {
    ///     let fluvio = Fluvio::connect().await?;
    ///     let admin = fluvio.admin().await;
    ///     admin.force_delete::<TopicSpec>(name).await?;
    ///     Ok(())
    /// }
    /// ```
    #[instrument(skip(self, key))]
    pub async fn force_delete<S>(&self, key: impl Into<S::DeleteKey>) -> Result<()>
    where
        S: DeletableAdminSpec + Sync + Send,
    {
        let delete_request: DeleteRequest<S> = DeleteRequest::with(key.into(), true);
        debug!("sending force delete request: {:#?}", delete_request);

        self.send_receive_admin::<ObjectApiDeleteRequest, _>(delete_request)
            .await?
            .as_result()?;
        Ok(())
    }

    /// Update object by key
    /// key is dependent on spec, most are string but some allow multiple types
    #[instrument(skip(self, key))]
    pub async fn update<S>(
        &self,
        key: impl Into<S::UpdateKey>,
        action: S::UpdateAction,
    ) -> Result<()>
    where
        S: UpdatableAdminSpec + Sync + Send,
    {
        let update_request: UpdateRequest<S> = UpdateRequest::new(key.into(), action);
        debug!("sending update request: {:#?}", update_request);

        self.send_receive_admin::<ObjectApiUpdateRequest, _>(update_request)
            .await?
            .as_result()?;
        Ok(())
    }

    /// return all instance of this spec
    #[instrument(skip(self))]
    pub async fn all<S>(&self) -> Result<Vec<Metadata<S>>>
    where
        S: AdminSpec,
        S::Status: Encoder + Decoder + Debug,
    {
        self.list_with_params::<S, String>(vec![], false).await
    }

    /// return all instance of this spec by filter
    #[instrument(skip(self, filters))]
    pub async fn list<S, F>(&self, filters: Vec<F>) -> Result<Vec<Metadata<S>>>
    where
        S: AdminSpec,
        ListFilter: From<F>,
        S::Status: Encoder + Decoder + Debug,
    {
        self.list_with_params(filters, false).await
    }

    #[instrument(skip(self, filters))]
    pub async fn list_with_params<S, F>(
        &self,
        filters: Vec<F>,
        summary: bool,
    ) -> Result<Vec<Metadata<S>>>
    where
        S: AdminSpec,
        ListFilter: From<F>,
        S::Status: Encoder + Decoder + Debug,
    {
        let filter_list: Vec<ListFilter> = filters.into_iter().map(Into::into).collect();
        let list_request: ListRequest<S> = ListRequest::new(filter_list, summary);

        self.list_with_config(list_request).await
    }

    #[instrument(skip(self, config))]
    pub async fn list_with_config<S, F>(&self, config: ListRequest<S>) -> Result<Vec<Metadata<S>>>
    where
        S: AdminSpec,
        ListFilter: From<F>,
        S::Status: Encoder + Decoder + Debug,
    {
        let response = self
            .send_receive_admin::<ObjectApiListRequest, _>(config)
            .await?;
        trace!("list response: {:#?}", response);
        response
            .downcast()?
            .ok_or(anyhow!("downcast error: {s}", s = S::LABEL))
            .map(|out: ListResponse<S>| out.inner())
    }

    /// Watch stream of changes for metadata
    /// There is caching, this is just pass through
    #[instrument(skip(self))]
    pub async fn watch<S>(&self) -> Result<impl Stream<Item = Result<WatchResponse<S>, IoError>>>
    where
        S: AdminSpec,
        S::Status: Encoder + Decoder,
    {
        // only summary for watch
        let watch_request: WatchRequest<S> = WatchRequest::summary();
        let version = self
            .socket
            .lookup_version::<ObjectApiWatchRequest>()
            .ok_or(anyhow!(
                "no version found watch request {}",
                ObjectApiWatchRequest::API_KEY
            ))?;

        let watch_req = ObjectApiWatchRequest::try_encode_from(watch_request, version)?;
        let req_msg = RequestMessage::new_request(watch_req);
        debug!(api_version = req_msg.header.api_version(), obj = %S::LABEL, "create watch stream");
        let inner_socket = self.socket.new_socket();
        let stream = inner_socket.create_stream(req_msg, 10).await?;
        Ok(stream.map(|respons_result| match respons_result {
            Ok(response) => {
                let watch_response = response.downcast().map_err(|err| {
                    IoError::new(ErrorKind::Other, format!("downcast error: {:#?}", err))
                })?;
                watch_response.ok_or(IoError::new(
                    ErrorKind::Other,
                    format!("cannot decoded as {s}", s = S::LABEL),
                ))
            }
            Err(err) => Err(IoError::new(
                ErrorKind::Other,
                format!("socket error {err}"),
            )),
        }))
    }
}

/// API for streaming cached metadata
#[cfg(feature = "unstable")]
mod unstable {
    use super::*;
    use fluvio_stream_dispatcher::metadata::local::LocalMetadataItem;
    use futures_util::Stream;
    use crate::metadata::topic::TopicSpec;
    use crate::metadata::partition::PartitionSpec;
    use crate::metadata::spu::SpuSpec;
    use crate::metadata::store::MetadataChanges;

    impl FluvioAdmin {
        /// Create a stream that yields updates to Topic metadata
        pub fn watch_topics(
            &self,
        ) -> impl Stream<Item = MetadataChanges<TopicSpec, LocalMetadataItem>> {
            self.metadata.topics().watch()
        }

        /// Create a stream that yields updates to Partition metadata
        pub fn watch_partitions(
            &self,
        ) -> impl Stream<Item = MetadataChanges<PartitionSpec, LocalMetadataItem>> {
            self.metadata.partitions().watch()
        }

        /// Create a stream that yields updates to SPU metadata
        pub fn watch_spus(
            &self,
        ) -> impl Stream<Item = MetadataChanges<SpuSpec, LocalMetadataItem>> {
            self.metadata.spus().watch()
        }
    }
}
