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
//! # use fluvio_experimental::{FluvioClient, FluvioError};
//! # async fn do_example() -> Result<(), FluvioError> {
//! let mut fluvio = FluvioClient::new().await?;
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

use std::ops::RangeBounds;
use fluvio::{ClusterSocket, ClientError, Consumer, Producer};
use fluvio::config::ConfigFile;
use fluvio::metadata::topic::{TopicSpec, TopicReplicaParam};
use fluvio::metadata::partition::ReplicaKey;
use fluvio::spu::SpuPool;

type Offset = i64;

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

/// An interface for interacting with Fluvio streaming
pub struct FluvioClient {
    config: ConfigFile,
    socket: ClusterSocket,
}

impl FluvioClient {
    /// Creates a new Fluvio client with default configurations
    pub async fn new() -> Result<Self, FluvioError> {
        let config_file = FluvioConfig::load_default_or_new()?;
        Self::from_config(config_file).await
    }

    /// Creates a new Fluvio client with the given configuration
    pub async fn from_config(config_file: FluvioConfig) -> Result<Self, FluvioError> {
        let config = config_file.config();
        let cluster_config = config.current_cluster()
            .ok_or(FluvioError::ClusterSocket)?;
        let socket = ClusterSocket::connect(cluster_config).await?;
        Ok(Self {
            config: config_file,
            socket,
        })
    }

    /// Creates a new Topic with the given options
    ///
    /// # Example
    ///
    /// The simplest way to create a Topic is to just give it a name.
    /// This will use default configuration options.
    ///
    /// ```no_run
    /// # use fluvio_experimental::{FluvioClient, Topic, FluvioError};
    /// # async fn do_create_topic(fluvio: &mut FluvioClient) -> Result<(), FluvioError> {
    /// let topic: Topic = fluvio.create_topic("my-topic").await.unwrap();
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
    /// # use fluvio_experimental::{FluvioClient, TopicConfig, Topic, FluvioError};
    /// # async fn do_create_topic(fluvio: &mut FluvioClient) -> Result<(), FluvioError> {
    /// let topic_config = TopicConfig::new("my-topic")
    ///     .with_partitions(2)
    ///     .with_replicas(3);
    /// let topic: Topic = fluvio.create_topic(topic_config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_topic<T: Into<TopicConfig>>(
        &mut self,
        topic_config: T,
    ) -> Result<Topic, FluvioError> {
        let config = topic_config.into();
        let topic_spec = config.to_spec();
        let mut admin = self.socket.admin().await;
        admin.create(config.name.clone(), false, topic_spec).await?;
        let pool = self.socket.init_spu_pool().await?;
        Ok(Topic {
            config,
            pool,
        })
    }

    /// Queries Fluvio for the existing Topics and returns them
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_experimental::{FluvioError, FluvioClient};
    /// # async fn do_get_topics(client: &mut FluvioClient) -> Result<(), FluvioError> {
    /// let topics = client.get_topics().await?;
    /// for topic in &topics {
    ///     println!("Found topic: {}", topic.get_name());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_topics(&mut self) -> Result<Vec<TopicDetails>, FluvioError> {
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

/// Configuration options for connecting to Fluvio
pub type FluvioConfig = fluvio::config::ConfigFile;

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

/// A Topic is a logical unit of streaming that reflects your application's domain
pub struct Topic {
    config: TopicConfig,
    pool: SpuPool,
}

impl Topic {
    /// Sends a String as a message to a partition in this `Topic`
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_experimental::{FluvioError, Topic};
    /// # async fn do_produce_message(topic: &mut Topic) -> Result<(), FluvioError> {
    /// topic.produce_string_message(0, "Hello, Fluvio!").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn produce_string_message<S: Into<String>>(&mut self, partition: i32, message: S) -> Result<(), FluvioError> {
        let replica = ReplicaKey {
            topic: self.config.name.clone(),
            partition,
        };
        let mut producer = Producer::new(replica, self.pool.clone());
        let message = message.into();
        producer.send_record(message.into_bytes()).await?;
        Ok(())
    }

    // NOTE: In the future it'd be nice to have some sort of Trait to represent
    // Fluvio message types. At that point in time, we can create new methods with
    // better names such as `produce` and `consume`. These v1 methods are intentionally
    // overly-specific to refer to Strings so that we can deprecate them in the
    // future without making breaking changes.
    /// Sends a String as a message to this Topic
    ///
    /// Messages must be accompanied by a "key", which is used to determine which
    /// partition will be responsible for processing this message. A key should
    /// be a piece of information derived from the message itself.
    ///
    /// For example, let's say you're keeping track of the transactions for a bank
    /// account. Each message represents a credit or debit to a certain account.
    /// Let's say your message is formatted as JSON and looks like the following:
    ///
    /// ```json
    /// {
    ///     "account": "alice",
    ///     "credit": 100
    /// }
    /// ```
    ///
    /// In your banking system, you want to make sure that all of the transactions
    /// for a particular account are processed in order. In order to achieve this,
    /// all of the events belonging to a particular account must all be processed
    /// by the same partition. In order for Fluvio to know how to keep all of the
    /// events from that account together, you should use the account name as the
    /// key when producing those messages to the topic.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use serde::Serialize;
    /// use fluvio_experimental::{Topic, FluvioError};
    ///
    /// #[derive(Serialize)]
    /// struct BankMessage {
    ///     account: String,
    ///     credit: u32,
    /// }
    ///
    /// async fn produce_my_message(topic: &mut Topic) -> Result<(), FluvioError> {
    ///     let transaction = BankMessage {
    ///         account: "alice".to_string(),
    ///         credit: 100,
    ///     };
    ///     let message = serde_json::to_string(&transaction).unwrap();
    ///     topic.produce_string_message_by_key(&transaction.account, message).await?;
    ///     Ok(())
    /// }
    /// ```
    #[cfg(feature = "unstable")]
    pub async fn produce_string_message_by_key<S: Into<String>>(
        &mut self,
        key: &str,
        message: S,
    ) -> Result<(), FluvioError> {
        let partition = 0; // TODO calculate partition from key
        self.produce_string_message(partition, message).await?;
        Ok(())
    }

    // // NOTE: In the future it'd be nice to have some sort of Trait to represent
    // // Fluvio message types. At that point in time, we can create new methods with
    // // better names such as `produce` and `consume`. These v1 methods are intentionally
    // // overly-specific to refer to Strings so that we can deprecate them in the
    // // future without making breaking changes.
    // /// Creates a Stream that yields new String messages as they arrive on this Topic
    // ///
    // /// # Example
    // ///
    // /// ```no_run
    // /// # use futures::StreamExt;
    // /// # use fluvio_experimental::Topic;
    // /// # async fn do_consume_string_messages(topic: &Topic) {
    // /// let mut consumer_stream = topic.consume_string_messages();
    // /// while let Some(event) = consumer_stream.next().await {
    // ///     println!("Received event: {}", event);
    // /// }
    // /// # }
    // /// ```
    // pub fn consume_string_messages(&self) -> TopicStringConsumer {
    //     todo!()
    // }

    // /// Fetch a batch of messages in a given range
    // ///
    // /// # Example
    // ///
    // /// ```no_run
    // /// # use fluvio_experimental::FluvioClient;
    // /// # async {
    // /// let client = FluvioClient::new().unwrap();
    // /// let topic = client.create_topic("my-topic").await.unwrap();
    // ///
    // /// // Fetching uses the Range syntax.
    // /// topic.fetch_string_messages(-10..);
    // /// # };
    // /// ```
    // pub async fn fetch_string_messages<R: RangeBounds<Offset>>(
    //     &mut self,
    //     range: R,
    // ) -> Result<Vec<String>, FluvioError> {
    //     let consumer = Consumer {
    //         pool: self.pool.clone(),
    //         replica:
    //     }
    //     todo!()
    // }

    /// Creates a handle for interacting with a given Partition of this Topic.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_experimental::{Topic, FluvioError};
    /// # async fn do_partition(topic: &mut Topic) -> Result<(), FluvioError> {
    /// let partition = topic.partition(0)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn partition(&mut self, partition: i32) -> Result<Partition, FluvioError> {
        let replica = ReplicaKey {
            topic: self.config.name.clone(),
            partition,
        };
        Ok(Partition {
            topic: self.config.name.clone(),
            pool: self.pool.clone(),
            replica,
            partition,
        })
    }

    #[cfg(feature = "unstable")]
    fn partition_by_key(&self, key: &str) -> Partition {
        todo!()
    }
}

/// A Partition is a unit of streaming that enables messages to be sent in parallel.
///
/// You can think of a partition as a fraction of a `Topic`. Every message that is
/// produced to a topic will be assigned to a partition, which is responsible for
/// streaming and storing that message. The thing that makes partitions special is
/// that separate partitions may be processed by entirely different machines. This
/// ultimately allows you to increase the potential throughput of your topic.
///
/// ```text
/// +-------+-----------+
/// |       | Partition |
/// | Topic |-----------+
/// |       | Partition |
/// +-------+-----------+
/// ```
///
/// Having more than one partition for a topic changes the ordering semantics of
/// your topic. All of the messages belonging to a single partition will arrive and
/// be stored in order. However, there is no ordering guarantee for messages that
/// belong to different partitions.
///
/// # Example
///
/// Suppose you have a service for scanning admissions tickets at sporting events.
/// Your system has a barcode scanner that reads a number from each ticket and
/// produces them to a topic called `admissions`. Lets say this topic has two
/// partitions, and that you send all of the even ticket numbers to one partition
/// and all of the odd ticket numbers to the other partition.
///
/// ```text
/// +--------------------+----------------------------+
/// |                    | Partition 0 (even tickets) |
/// | Topic (admissions) |----------------------------+
/// |                    | Partition 1 (odd tickets)  |
/// +--------------------+----------------------------+
/// ```
///
/// The code to produce ticket numbers from your barcode scanner to the `admissions`
/// topic might look something like this:
///
/// ```no_run
/// # struct Barcode;
/// # impl Barcode {
/// #     async fn next(&mut self) -> Option<u32> { unimplemented!() }
/// # }
/// # use fluvio_experimental::{FluvioError, Topic};
/// # async fn do_produce_tickets(admissions_topic: &mut Topic, barcode_scanner: &mut Barcode) -> Result<(), FluvioError> {
/// let mut even_partition = admissions_topic.partition(0)?;
/// let mut odd_partition = admissions_topic.partition(1)?;
///
/// while let Some(ticket_number) = barcode_scanner.next().await {
///     if ticket_number % 2 == 0 {
///         even_partition.produce_string_message(format!("{}", ticket_number)).await?;
///     } else {
///         odd_partition.produce_string_message(format!("{}", ticket_number)).await?;
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Game night arrives, and your first few fans in line have the following ticket numbers
///
/// ```text
/// 12, 13, 55, 89, 90, 44, 23
/// ```
///
/// Those ticket numbers would be sent to the `admissions` topic like this
///
/// ```text
/// +--------------------+----------------------------+
/// |                    | Partition 0 (even tickets) |
/// |                    | [ 12, 90, 44 ]             |
/// | Topic (admissions) |----------------------------+
/// |                    | Partition 1 (odd tickets)  |
/// |                    | [ 13, 55, 89, 23 ]         |
/// +--------------------+----------------------------+
/// ```
///
/// Notice that all of the even-numbered tickets are stored in the order that they
/// arrived in, and that the same is true for the odd-numbered tickets. However, it is
/// important to realize that there is no longer any ordering guarantee between even
/// and odd tickets. It would be impossible to tell whether ticket `12` arrived before
/// or after ticket `89`.
pub struct Partition {
    topic: String,
    pool: SpuPool,
    replica: ReplicaKey,
    partition: i32,
}

impl Partition {
    // pub fn consume_string_messages(&self) -> Result<AsyncResponse<DefaultStreamFetchRequest>, FluvioError> {
    //     let mut consumer = Consumer::new(self.replica.clone(), self.pool.clone());
    //     let stream = consumer.fetch_logs_as_stream().await?;
    // }

    pub async fn fetch_string_messages<R: RangeBounds<Offset>>(&self, range: R) -> Result<Vec<String>, FluvioError> {
        todo!()
    }

    /// Produces a string message to this partition
    pub async fn produce_string_message<S: Into<String>>(&mut self, message: S) -> Result<(), FluvioError> {
        let mut producer = Producer::new(self.replica.clone(), self.pool.clone());
        producer.send_record(message.into().into_bytes()).await?;
        Ok(())
    }
}
