use std::convert::{TryFrom, TryInto};
use std::fmt::Display;

use tracing::debug;
use dataplane::core::Encoder;
use dataplane::core::Decoder;
use fluvio_sc_schema::objects::{Metadata, AllCreatableSpec};
use fluvio_sc_schema::AdminRequest;

#[cfg(not(target_arch = "wasm32"))]
use fluvio_socket::AllMultiplexerSocket as AllMultiplexerSocket;
#[cfg(not(target_arch = "wasm32"))]
use fluvio_future::native_tls::AllDomainConnector as FluvioConnector;

use crate::sockets::{ClientConfig, VersionedSerialSocket, SerialFrame};
use crate::{FluvioError, FluvioConfig};
use crate::metadata::objects::{ListResponse, ListSpec, DeleteSpec, CreateRequest};
use crate::config::ConfigFile;

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
pub struct FluvioAdmin(VersionedSerialSocket);

impl FluvioAdmin {
    pub(crate) fn new(client: VersionedSerialSocket) -> Self {
        Self(client)
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
    pub async fn connect_with_config(config: &FluvioConfig) -> Result<Self, FluvioError> {
        let connector = FluvioConnector::try_from(config.tls.clone())?;
        let config = ClientConfig::new(&config.endpoint, connector);
        let inner_client = config.connect().await?;
        debug!("connected to cluster at: {}", inner_client.config().addr());

        let (socket, config, versions) = inner_client.split();
        let socket = AllMultiplexerSocket::shared(socket);

        let versioned_socket = VersionedSerialSocket::new(socket, config, versions);
        Ok(Self(versioned_socket))
    }

    async fn send_receive<R>(&mut self, request: R) -> Result<R::Response, FluvioError>
    where
        R: AdminRequest + Send + Sync,
    {
        self.0.send_receive(request).await
    }

    /// create new object
    pub async fn create<S>(
        &mut self,
        name: String,
        dry_run: bool,
        spec: S,
    ) -> Result<(), FluvioError>
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
    pub async fn delete<S, K>(&mut self, key: K) -> Result<(), FluvioError>
    where
        S: DeleteSpec,
        K: Into<S::DeleteKey>,
    {
        let delete_request = S::into_request(key);
        self.send_receive(delete_request).await?.as_result()?;
        Ok(())
    }

    pub async fn list<S, F>(&mut self, filters: F) -> Result<Vec<Metadata<S>>, FluvioError>
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

    /*
    /// Connect to replica leader for a topic/partition
    async fn find_replica_for_topic_partition(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<Self::Leader, ClientError> {
        debug!(
            "trying to find replica for topic: {}, partition: {}",
            topic, partition
        );

        let topic_comp_resp = self.get_topic_composition(topic).await?;

        trace!("topic composition: {:#?}", topic_comp_resp);

        let mut topics_resp = topic_comp_resp.topics;
        let spus_resp = topic_comp_resp.spus;

        // there must be one topic in reply
        if topics_resp.len() != 1 {
            return Err(ClientError::IoError(IoError::new(
                ErrorKind::InvalidData,
                format!("topic error: expected 1 topic, found {}", topics_resp.len()),
            )));
        }

        let topic_resp = topics_resp.remove(0);

        if topic_resp.error_code != FlvErrorCode::None {
            if topic_resp.error_code == FlvErrorCode::TopicNotFound {
                return Err(ClientError::TopicNotFound(topic.to_owned()));
            } else {
                return Err(ClientError::IoError(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "error during topic lookup: {}",
                        topic_resp.error_code.to_sentence()
                    ),
                )));
            }
        }
        // lookup leader
        for partition_resp in topic_resp.partitions {
            if partition_resp.partition_idx == partition {
                // check for errors
                if partition_resp.error_code != FlvErrorCode::None {
                    return Err(ClientError::IoError(IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "topic-composition partition error: {}",
                            topic_resp.error_code.to_sentence()
                        ),
                    )));
                }

                // traverse spus and find leader
                let leader_id = partition_resp.leader_id;
                for spu_resp in &spus_resp {
                    if spu_resp.spu_id == leader_id {
                        // check for errors
                        if spu_resp.error_code != FlvErrorCode::None {
                            return Err(ClientError::IoError(IoError::new(
                                ErrorKind::InvalidData,
                                format!(
                                    "problem with partition look up {}:{} error: {}",
                                    topic,
                                    partition,
                                    topic_resp.error_code.to_sentence()
                                ),
                            )));
                        }

                        debug!("spu {}/{}: is leader", spu_resp.host, spu_resp.port);

                        let mut leader_client_config = self.0.config().clone();
                        let addr: ServerAddress = spu_resp.into();
                        leader_client_config.set_addr(addr.to_string());

                        let client = leader_client_config.connect().await?;
                        let leader_config = ReplicaLeaderConfig::new(topic.to_owned(), partition);
                        return Ok(SpuReplicaLeader::new(leader_config, client));
                    }
                }
            }
        }

        Err(ClientError::PartitionNotFound(topic.to_owned(), partition))
    }
    */
}
