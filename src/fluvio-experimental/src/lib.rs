use futures::Stream;
use futures::task::{Context, Poll};
use std::pin::Pin;
use std::ops::{Range, RangeTo, RangeFrom, RangeToInclusive, RangeFull, RangeBounds};

/// Possible errors that may arise when using Fluvio
#[derive(Debug)]
pub enum FluvioError {}

/// An interface for interacting with Fluvio streaming
pub struct FluvioClient {}

impl FluvioClient {
    /// Creates a new Fluvio client with default configurations
    pub fn new() -> Result<Self, FluvioError> {
        todo!()
    }

    /// Creates a new Fluvio client with the given configuration
    pub fn from_config(config: FluvioConfig) -> Result<Self, FluvioError> {
        todo!()
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
    pub async fn create_topic<T: Into<TopicConfig>>(&self, config: T) -> Result<Topic, FluvioError> {
        todo!()
    }

    /// Queries Fluvio for the existing Topics and returns them
    pub async fn get_topics(&self) -> Result<Vec<TopicConfig>, FluvioError> {
        todo!()
    }
}

/// Configuration options for connecting to Fluvio
pub struct FluvioConfig {}

/// Describes configuration options for a new or existing Topic
pub struct TopicConfig {
    name: String,
    partitions: u16,
    replication: u16,
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
    pub fn with_partitions(mut self, partitions: u16) -> Self {
        self.partitions = partitions;
        self
    }

    /// Sets the number of replicas for this Topic to be copied into
    pub fn with_replicas(mut self, replicas: u16) -> Self {
        self.replication = replicas;
        self
    }
}

/// A handle to a Fluvio Topic, which may be streamed from or to
pub struct Topic {
    config: TopicConfig,
}

impl Topic {
    // NOTE: In the future it'd be nice to have some sort of Trait to represent
    // Fluvio message types. At that point in time, we can create new methods with
    // better names such as `produce` and `consume`. These v1 methods are intentionally
    // overly-specific to refer to Strings so that we can deprecate them in the
    // future without making breaking changes.
    /// Sends a String as a message to this Topic
    pub async fn produce_string_message<S: Into<String>>(&self, message: S) -> Result<(), FluvioError> {
        todo!()
    }

    // NOTE: In the future it'd be nice to have some sort of Trait to represent
    // Fluvio message types. At that point in time, we can create new methods with
    // better names such as `produce` and `consume`. These v1 methods are intentionally
    // overly-specific to refer to Strings so that we can deprecate them in the
    // future without making breaking changes.
    /// Creates a Stream that yields new String messages as they arrive on this Topic
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
    pub async fn fetch_string_messages<R: RangeBounds<Offset>>(&self, range: R) -> Result<Vec<String>, FluvioError> {
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

type Offset = u64;
