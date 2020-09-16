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
//! # use fluvio_experimental::FluvioClient;
//! let client = FluvioClient::new().unwrap();
//! async_std::task::block_on(async {
//!     // Create a new Fluvio topic called "echo"
//!     let topic = client.create_topic("echo").await.unwrap();
//!     // Send a message to the echo topic
//!     topic.produce_string_message("Hello, Fluvio!").await.unwrap();
//!     // Fetch all of the messages in the "echo" topic and print them
//!     let messages = topic.fetch_string_messages(..).await.unwrap();
//!     for message in &messages {
//!         println!("{}", message);
//!     }
//! });
//! ```
#![cfg_attr(
feature = "nightly",
doc(include = "../../../website/kubernetes/INSTALL.md")
)]

use std::pin::Pin;
use std::ops::RangeBounds;
use futures::Stream;
use futures::task::{Context, Poll};
use fluvio::{ClusterSocket, ClientError};
use fluvio::config::ConfigFile;
use fluvio::metadata::topic::{TopicSpec, TopicReplicaParam};

type Offset = u64;

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
    /// # use fluvio_experimental::{FluvioClient, Topic};
    /// # async {
    /// let client = FluvioClient::new().unwrap();
    /// let topic: Topic = client.create_topic("my-topic").await.unwrap();
    /// # };
    /// ```
    ///
    /// # Example
    ///
    /// If you want to customize your Topic's configuration, you can
    /// pass it a full `TopicConfig`.
    ///
    /// ```no_run
    /// # use fluvio_experimental::{FluvioClient, TopicConfig, Topic};
    /// # async {
    /// let client = FluvioClient::new().unwrap();
    /// let topic_config = TopicConfig::new("my-topic")
    ///     .with_partitions(2)
    ///     .with_replicas(3);
    /// let topic: Topic = client.create_topic(topic_config).await.unwrap();
    /// # };
    /// ```
    pub async fn create_topic<T: Into<TopicConfig>>(
        &mut self,
        topic_config: T,
    ) -> Result<Topic, FluvioError> {
        let config = topic_config.into();
        let topic_spec = config.to_spec();
        let mut admin = self.socket.admin().await;
        admin.create(config.name.clone(), false, topic_spec).await?;
        Ok(Topic {
            config,
            socket: &mut self.socket,
        })
    }

    /// Queries Fluvio for the existing Topics and returns them
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

#[derive(Debug)]
pub struct TopicDetails {
    name: String,
    partitions: Option<i32>,
    replication: Option<i32>,
}

/// A handle to a Fluvio Topic, which may be streamed from or to
pub struct Topic<'a> {
    config: TopicConfig,
    socket: &'a mut ClusterSocket,
}

impl Topic {
    // NOTE: In the future it'd be nice to have some sort of Trait to represent
    // Fluvio message types. At that point in time, we can create new methods with
    // better names such as `produce` and `consume`. These v1 methods are intentionally
    // overly-specific to refer to Strings so that we can deprecate them in the
    // future without making breaking changes.
    /// Sends a String as a message to this Topic
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fluvio_experimental::Topic;
    /// async fn produce_my_message(topic: &Topic) {
    ///     topic.produce_string_message("Hello, Fluvio!").await.unwrap();
    /// }
    /// ```
    pub async fn produce_string_message<S: Into<String>>(
        &self,
        message: S,
    ) -> Result<(), FluvioError> {
        todo!()
    }

    // NOTE: In the future it'd be nice to have some sort of Trait to represent
    // Fluvio message types. At that point in time, we can create new methods with
    // better names such as `produce` and `consume`. These v1 methods are intentionally
    // overly-specific to refer to Strings so that we can deprecate them in the
    // future without making breaking changes.
    /// Creates a Stream that yields new String messages as they arrive on this Topic
    ///
    /// # Example
    ///
    /// ```no_run
    /// use futures::StreamExt;
    /// use fluvio_experimental::Topic;
    /// async fn print_messages(topic: &Topic) {
    ///     let mut consumer_stream = topic.consume_string_messages();
    ///     while let Some(event) = consumer_stream.next().await {
    ///         println!("Received event: {}", event);
    ///     }
    /// }
    /// ```
    pub fn consume_string_messages(&self) -> TopicStringConsumer {
        todo!()
    }

    /// Fetch a batch of messages in a given range
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_experimental::FluvioClient;
    /// # async {
    /// let client = FluvioClient::new().unwrap();
    /// let topic = client.create_topic("my-topic").await.unwrap();
    /// topic.fetch_string_messages(..100);
    /// # };
    /// ```
    pub async fn fetch_string_messages<R: RangeBounds<Offset>>(
        &self,
        range: R,
    ) -> Result<Vec<String>, FluvioError> {
        todo!()
    }

    pub fn partition(&self, partition: u16) -> Partition {
        Partition {
            partition,
            topic: self.config.name.clone(),
        }
    }

    pub fn partition_by_key(&self, key: &str) -> Partition {
        todo!()
    }
}

/// A Stream that yields new events received from a specific Topic
pub struct TopicStringConsumer {}

impl Stream for TopicStringConsumer {
    type Item = String;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub struct Partition {
    topic: String,
    partition: u16,
}

impl Partition {
    pub fn consume_string_messages(&self) -> PartitionStringConsumer {
        todo!()
    }

    pub async fn fetch_string_messages<R: RangeBounds<Offset>>(&self, range: R) -> Result<Vec<String>, FluvioError> {
        todo!()
    }
}

pub struct PartitionStringConsumer {}

impl Stream for PartitionStringConsumer {
    type Item = String;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
