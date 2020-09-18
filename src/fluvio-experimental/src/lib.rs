#![cfg_attr(feature = "nightly", feature(external_doc))]

//! The official Rust client library for writing streaming applications with Fluvio
//!
//! Fluvio is a high performance, low latency data streaming platform built for developers.
//!
//! When writing streaming applications, two of your core behaviors are producing messages
//! and consuming messages. When you produce a message, you send it to a Fluvio cluster
//! where it is recorded and saved for later usage. When you consume a message, you are
//! reading a previously-stored message from that same Fluvio cluster. Let's get started
//! with a quick example where we produce and consume some messages.
//!
//! # Prerequisites
//!
//! [Install Fluvio](#installation)
//!
//! # Fluvio Echo
//!
//! The easiest way to see Fluvio in action is to produce some messages and to consume
//! them right away. In this sense, we can use Fluvio to make an "echo service".
//!
//! All messages in Fluvio are sent in a sort of category called a `Topic`. You can think
//! of a Topic as a named folder where you want to store some files, which would be your
//! messages. If you're familiar with relational databases, you can think of a Topic as
//! being similar to a database table, but for streaming.
//!
//! As the application developer, you get to decide what Topics you create and which
//! messages you send to them. For the echo example, we'll create a Topic called `echo`.
//!
//! ```no_run
//! # use fluvio_experimental::{Fluvio, FluvioError};
//! # async fn do_example() -> Result<(), FluvioError> {
//! let mut fluvio = Fluvio::connect().await?;
//! // Create a new Fluvio topic called "echo"
//! let mut topic = fluvio.create_topic("echo").await?;
//! // Send a message to the echo topic
//! topic.produce_string_message(0, "Hello, Fluvio!").await?;
//! // Fetch all of the messages in the "echo" topic and print them
//! let messages = topic.fetch_string_messages(..).await?;
//! for message in &messages {
//!     println!("{}", message);
//! }
//! # Ok(())
//! # }
//! ```
#![cfg_attr(
feature = "nightly",
doc(include = "../../../website/kubernetes/INSTALL.md")
)]

use fluvio::{AdminClient, ClusterSocket, ClientError, Consumer, Producer as InternalProducer};
use fluvio::config::ConfigFile;
use fluvio::metadata::topic::{TopicSpec, TopicReplicaParam};
use fluvio::metadata::partition::ReplicaKey;
use fluvio::spu::SpuPool;
use futures::AsyncWrite;

/// Possible errors that may arise when using Fluvio
#[derive(Debug)]
pub enum FluvioError {
    ClusterSocket,
    ClientError(ClientError),
    IoError(std::io::Error),
}

impl From<std::io::Error> for FluvioError {
    fn from(error: std::io::Error) -> Self {
        Self::IoError(error)
    }
}

impl From<ClientError> for FluvioError {
    fn from(error: ClientError) -> Self {
        Self::ClientError(error)
    }
}

/// Creates a `PartitionProducer` for sending events to Fluvio.
///
/// # Example
///
/// ```no_run
/// # use fluvio_experimental as fluvio;
/// # use fluvio_experimental::FluvioError;
/// # async fn do_get_producer() -> Result<(), FluvioError> {
/// let producer = fluvio::partition_producer("my-topic", 0).await?;
/// producer.produce_buffer("Hello, world!").await?;
/// # Ok(())
/// # }
/// ```
pub async fn partition_producer<S: Into<String>>(topic: S, partition: i32) -> Result<PartitionProducer, FluvioError> {
    let mut fluvio = Fluvio::connect().await?;
    let producer = fluvio.partition_producer(topic, partition)?;
    Ok(producer)
}

/// Creates a `PartitionConsumer` for receiving events from Fluvio
///
/// # Example
///
/// ```no_run
/// # use fluvio_experimental as fluvio;
/// # use fluvio_experimental::FluvioError;
/// # async fn do_get_consumer() -> Result<(), FluvioError> {
/// let consumer = fluvio::partition_consumer("my-topic", 0).await?;
/// # Ok(())
/// # }
/// ```
pub async fn partition_consumer<S: Into<String>>(topic: S, partition: i32) -> Result<PartitionConsumer, FluvioError> {
    let mut fluvio = Fluvio::connect().await?;
    let consumer = fluvio.partition_consumer(topic, partition)?;
    Ok(consumer)
}

/// An interface for interacting with Fluvio streaming
pub struct Fluvio {
    config: ConfigFile,
    socket: ClusterSocket,
}

impl Fluvio {
    /// Creates a new Fluvio client with default configurations
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_experimental::{Fluvio, FluvioError};
    /// # async fn do_connect() -> Result<(), FluvioError> {
    /// let fluvio = Fluvio::connect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect() -> Result<Self, FluvioError> {
        let config_file = FluvioConfig::load_default_or_new()?;
        Self::connect_with_config(config_file).await
    }

    /// Creates a new Fluvio client with the given configuration
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_experimental::{Fluvio, FluvioError, FluvioConfig};
    /// # async fn do_connect_with_config() -> Result<(), FluvioError> {
    /// let config = FluvioConfig::load_default_or_new()?;
    /// let fluvio = Fluvio::connect_with_config(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_config(config_file: FluvioConfig) -> Result<Self, FluvioError> {
        let config = config_file.config();
        let cluster_config = config.current_cluster()
            .ok_or(FluvioError::ClusterSocket)?;
        let socket = ClusterSocket::connect(cluster_config).await?;
        Ok(Self {
            config: config_file,
            socket,
        })
    }

    /// Provides an interface for managing a Fluvio cluster
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_experimental::{Fluvio, FluvioError};
    /// # async fn do_get_admin(fluvio: &mut Fluvio) -> Result<(), FluvioError> {
    /// let admin = fluvio.admin().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn admin(&mut self) -> Result<FluvioAdmin, FluvioError> {
        let admin = self.socket.admin().await;
        Ok(FluvioAdmin { admin })
    }

    /// Creates a producer that sends messages to a specific partition in a topic
    pub fn partition_producer<S: Into<String>>(&mut self, topic: S, partition: i32) -> Result<PartitionProducer, FluvioError> {
        let replica_key = ReplicaKey::new(topic, partition);
        let internal_producer = self.socket.producer(replica_key).await?;
        Ok(PartitionProducer { producer: internal_producer })
    }

    /// Creates a consumer that receives messages from a specific partition in a topic
    pub fn partition_consumer<S: Into<String>>(&mut self, topic: S, partition: i32) -> Result<PartitionConsumer, FluvioError> {
        let replica_key = ReplicaKey::new(topic, partition);
        let internal_consumer = self.socket.consumer(replica_key).await?;
        Ok(PartitionConsumer { consumer: internal_consumer })
    }
}

/// Configuration options for connecting to Fluvio
pub type FluvioConfig = fluvio::config::ConfigFile;

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
/// client via the regular `Fluvio` client.
///
/// # Example
///
/// Note that this may fail if you are not authorized as a Fluvio
/// administrator for the cluster you are connected to.
///
/// ```no_run
/// # use fluvio_experimental::{Fluvio, FluvioError};
/// # async fn do_get_admin(fluvio: &mut Fluvio) -> Result<(), FluvioError> {
/// let admin = fluvio.admin().await?;
/// # Ok(())
/// # }
/// ```
pub struct FluvioAdmin {
    admin: AdminClient,
}

impl FluvioAdmin {
    /// Creates a new Topic with the given options
    ///
    /// # Example
    ///
    /// The simplest way to create a Topic is to just give it a name.
    /// This will use default configuration options.
    ///
    /// ```no_run
    /// # use fluvio_experimental::{FluvioAdmin, FluvioError, TopicDetails};
    /// # async fn do_create_topic(fluvio_admin: &mut FluvioAdmin) -> Result<(), FluvioError> {
    /// let topic: TopicDetails = fluvio_admin.create_topic("my-topic").await.unwrap();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Example
    ///
    /// If you want to customize your Topic's configuration, you can
    /// pass it a full `TopicConfig`.
    ///
    /// ```no_run
    /// # use fluvio_experimental::{FluvioAdmin, FluvioError, TopicConfig, TopicDetails};
    /// # async fn do_create_topic(fluvio_admin: &mut FluvioAdmin) -> Result<(), FluvioError> {
    /// let topic_config = TopicConfig::new("my-topic")
    ///     .with_partitions(2)
    ///     .with_replicas(3);
    /// let topic: TopicDetails = fluvio_admin.create_topic(topic_config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_topic<T: Into<TopicConfig>>(
        &mut self,
        topic_config: T,
    ) -> Result<TopicDetails, FluvioError> {
        let config = topic_config.into();
        let topic_spec = config.to_spec();
        self.admin.create(config.name.clone(), false, topic_spec).await?;
        Ok(TopicDetails {
            name: config.name,
            partitions: topic_spec.partitions(),
            replication: topic_spec.replication_factor(),
        })
    }

    /// Queries Fluvio for the existing Topics and returns them
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_experimental::{FluvioAdmin, FluvioError};
    /// # async fn do_get_topics(fluvio_admin: &mut FluvioAdmin) -> Result<(), FluvioError> {
    /// let topics = fluvio_admin.list_topics().await?;
    /// for topic in &topics {
    ///     println!("Found topic: {}", topic.get_name());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_topics(&mut self) -> Result<Vec<TopicDetails>, FluvioError> {
        let mut admin = self.socket.admin().await;
        let metadata = admin.list::<TopicSpec, _>(vec![]).await?;
        let topics = metadata.into_iter()
            .map(|topic_metadata| {
                TopicDetails {
                    name: topic_metadata.name,
                    partitions: topic_metadata.spec.partitions(),
                    replication: topic_metadata.spec.replication_factor(),
                }
            })
            .collect();
        Ok(topics)
    }
}

/// Describes configuration options for a new or existing Topic
pub struct TopicConfig {
    name: String,
    partitions: i32,
    replication: i32,
}

impl<S: Into<String>> From<S> for TopicConfig {
    fn from(string: S) -> Self {
        Self::new(string)
    }
}

impl TopicConfig {
    /// Creates a Topic configuration with the given name and default options
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            partitions: 1,
            replication: 1,
        }
    }

    /// Sets the number of partitions for this Topic to be divided into
    pub fn with_partitions(mut self, partitions: i32) -> Self {
        self.partitions = partitions;
        self
    }

    /// Sets the number of replicas for this Topic to be copied into
    pub fn with_replicas(mut self, replicas: i32) -> Self {
        self.replication = replicas;
        self
    }

    /// Converts the public TopicConfig to the internal TopicSpec
    fn to_spec(&self) -> TopicSpec {
        TopicSpec::Computed(TopicReplicaParam::new(
            self.partitions,
            self.replication,
            false,
        ))
    }
}

/// A description of a Topic used to display to the user
#[derive(Debug)]
pub struct TopicDetails {
    name: String,
    partitions: Option<i32>,
    replication: Option<i32>,
}

impl TopicDetails {
    /// Returns the name of the described topic
    pub fn get_name(&self) -> &str {
        &self.name
    }
}

enum OffsetInner {
    Absolute(i64),
    FromBeginning(i64),
    FromEnd(i64),
}

/// Describes the location of an event stored in a Fluvio partition
///
/// All Fluvio events are stored as a log inside a partition. A log
/// is just an ordered list, and an `Offset` is just a way to select
/// an item from that list. There are several ways that an `Offset`
/// may identify an element from a log. Suppose you sent the first
/// 10 multiples of `11` to your partition. The various offset types
/// would look like this:
///
/// ```text
///         Partition Log: [ 00, 11, 22, 33, 44, 55, 66 ]
///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6
///  FromBeginning Offset:    0,  1,  2,  3,  4,  5,  6
///        FromEnd Offset:    6,  5,  4,  3,  2,  1,  0
/// ```
///
/// When a new partition is created, it always starts counting new
/// events at the `Absolute` offset of 0. An absolute offset is a
/// unique index that represents the event's distance from the very
/// beginning of the partition. The absolute offset of an event never
/// changes.
///
/// Sometimes when a partition gets very large, Fluvio will begin
/// deleting events from the beginning of the log in order to save
/// space. Whenever it does this, it keeps track of the latest
/// non-deleted event. This allows the `FromBeginning` offset to
/// select an event which is a certain number of places in front of
/// the deleted range. For example, let's say that the first two
/// events from our partition were deleted. Our new offsets would
/// look like this:
///
/// ```text
///                          These events were deleted!
///                          |
///                          vvvvvv
///         Partition Log: [ .., .., 22, 33, 44, 55, 66 ]
///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6
///  FromBeginning Offset:            0,  1,  2,  3,  4
///        FromEnd Offset:    6,  5,  4,  3,  2,  1,  0
/// ```
///
/// Just like the `FromBeginning` offset may change if events are deleted,
/// the `FromEnd` offset will change when new events are added. Let's take
/// a look:
///
/// ```text
///                                        These events were added!
///                                                               |
///                                                      vvvvvvvvvv
///         Partition Log: [ .., .., 22, 33, 44, 55, 66, 77, 88, 99 ]
///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6,  7,  8,  9
///  FromBeginning Offset:            0,  1,  2,  3,  4,  5,  6,  7
///        FromEnd Offset:    9,  8,  7,  6,  5,  4,  3,  2,  1,  0
/// ```
///
pub struct Offset {
    offset: OffsetInner,
}

impl Offset {
    /// Creates an absolute offset with the given index
    ///
    /// The index must not be less than zero.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_experimental::Offset;
    /// assert!(Offset::absolute(100).is_some());
    /// assert!(Offset::absolute(0).is_some());
    /// assert!(Offset::absolute(-10).is_none());
    /// ```
    pub fn absolute(index: i64) -> Option<Offset> {
        if index < 0 {
            return None;
        }
        Some(Self {
            offset: OffsetInner::Absolute(index),
        })
    }

    /// Creates a relative offset starting at the beginning of the saved log
    ///
    /// A relative `FromBeginning` offset will not always match an `Absolute`
    /// offset. In order to save space, Fluvio may sometimes delete events
    /// from the beginning of the log. When this happens, the `FromBeginning`
    /// relative offset starts counting from the first non-deleted log entry.
    ///
    /// ```text
    ///                          These events were deleted!
    ///                          |
    ///                          vvvvvv
    ///         Partition Log: [ .., .., 22, 33, 44, 55, 66 ]
    ///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6
    ///  FromBeginning Offset:            0,  1,  2,  3,  4
    /// ```
    ///
    /// The offset must not be less than zero.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_experimental::Offset;
    /// assert!(Offset::from_beginning(10).is_some());
    /// assert!(Offset::from_beginning(0).is_some());
    /// assert!(Offset::from_beginning(-10).is_none());
    /// ```
    pub fn from_beginning(offset: i64) -> Option<Offset> {
        if offset < 0 {
            return None;
        }
        Some(Self {
            offset: OffsetInner::FromBeginning(offset),
        })
    }

    /// Creates a relative offset counting backwards from the end of the log
    ///
    /// A relative `FromEnd` offset will begin counting from the last
    /// "stable committed" event entry in the log. Increasing the offset will
    /// select events in reverse chronological order from the most recent event
    /// towards the earliest event. Therefore, a relative `FromEnd` offset may
    /// refer to different entries depending on when a query is made.
    ///
    /// For example, `Offset::from_end(3)` will refer to the event with content
    /// `33` at this point in time:
    ///
    /// ```text
    ///         Partition Log: [ .., .., 22, 33, 44, 55, 66 ]
    ///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6
    ///        FromEnd Offset:    6,  5,  4,  3,  2,  1,  0
    /// ```
    ///
    /// But when these new events are added, `Offset::from_end(3)` will refer to
    /// the event with content `66`:
    ///
    /// ```text
    ///                                        These events were added!
    ///                                                               |
    ///                                                      vvvvvvvvvv
    ///         Partition Log: [ .., .., 22, 33, 44, 55, 66, 77, 88, 99 ]
    ///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6,  7,  8,  9
    ///        FromEnd Offset:    9,  8,  7,  6,  5,  4,  3,  2,  1,  0
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_experimental::Offset;
    /// assert!(Offset::from_end(10).is_some());
    /// assert!(Offset::from_end(0).is_some());
    /// assert!(Offset::from_end(-10).is_none());
    /// ```
    pub fn from_end(offset: i64) -> Option<Offset> {
        if offset < 0 {
            return None;
        }
        Some(Self {
            offset: OffsetInner::FromEnd(offset),
        })
    }
}

/// Doc TODO. Replaces FetchLogOption
pub struct FetchConfig {

}

/// A Producer that sends messages to a specific partition of a topic
pub struct PartitionProducer {
    producer: InternalProducer,
}

impl PartitionProducer {
    pub async fn produce_buffer<B: AsRef<[u8]>>(&self, buffer: B) -> Result<(), FluvioError> {
        let record = buffer.as_ref().to_owned();
        self.producer.send_record(record).await?;
        Ok(())
    }
}

/// Doc: TODO
pub struct PartitionConsumer {
    consumer: Consumer,
}

impl PartitionConsumer {
}
