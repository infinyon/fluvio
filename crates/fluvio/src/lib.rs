//! The Rust client library for writing streaming applications with Fluvio
//!
//! Fluvio is a high performance, low latency data streaming platform built for developers.
//!
//! When writing streaming applications, two of your core behaviors are producing messages
//! and consuming messages. When you produce a message, you send it to a Fluvio cluster
//! where it is recorded and saved for later usage. When you consume a message, you are
//! reading a previously-stored message from that same Fluvio cluster. Let's get started
//! with a quick example where we produce and consume some messages.
//!
//! # Examples
//!
//! Fluvio's documentation provide a set of examples for Rust.
//! You can visit the examples page [following this link](https://www.fluvio.io/api/official/rust/examples/).
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
//! messages you send to them. We need to set up a Topic before running our code. For the
//! echo example, we'll call our topic `echo`.
//!
//! # Example
//!
//! The easiest way to create a Fluvio Topic is by using the [Fluvio CLI].
//!
//! ```bash
//! $ fluvio topic create echo
//! topic "echo" created
//! ```
//!
//! There are convenience methods that let you get up-and-started quickly using default
//! configurations. Later if you want to customize your setup, you can directly use the
//! [`Fluvio`] client object.
//!
//! ```no_run
//! # mod futures {
//! #     pub use futures_util::stream::StreamExt;
//! # }
//! use std::time::Duration;
//! use fluvio::{Offset, FluvioError, RecordKey};
//! use futures::StreamExt;
//!
//! async_std::task::spawn(produce_records());
//! if let Err(e) = async_std::task::block_on(consume_records()) {
//!     println!("Error: {}", e);
//! }
//!
//! async fn produce_records() -> Result<(), FluvioError> {
//!     let producer = fluvio::producer("echo").await?;
//!     for i in 0..10u8 {
//!         producer.send(RecordKey::NULL, format!("Hello, Fluvio {}!", i)).await?;
//!         async_std::task::sleep(Duration::from_secs(1)).await;
//!     }
//!     Ok(())
//! }
//!
//! async fn consume_records() -> Result<(), FluvioError> {
//!     let consumer = fluvio::consumer("echo", 0).await?;
//!     let mut stream = consumer.stream(Offset::beginning()).await?;
//!
//!     while let Some(Ok(record)) = stream.next().await {
//!         let key = record.key().map(|key| String::from_utf8_lossy(key).to_string());
//!         let value = String::from_utf8_lossy(record.value()).to_string();
//!         println!("Got record: key={:?}, value={}", key, value);
//!     }
//!     Ok(())
//! }
//! ```
//!
//! [Fluvio CLI]: https://nightly.fluvio.io/docs/cli/
//! [`Fluvio`]: ./struct.Fluvio.html
#![cfg_attr(
    feature = "nightly",
    doc(include = "../../../website/kubernetes/INSTALL.md")
)]

mod error;
mod admin;
mod fluvio;
pub mod consumer;
mod producer;
mod offset;
mod sync;
pub mod spu;
pub mod metrics;
pub mod config;

use tracing::instrument;
pub use error::FluvioError;
pub use config::FluvioConfig;
pub use producer::{
    TopicProducerConfigBuilder, TopicProducerConfig, TopicProducer, RecordKey, ProduceOutput,
    FutureRecordMetadata, RecordMetadata, DeliverySemantic, RetryPolicy, RetryStrategy,
};

pub use consumer::{
    PartitionConsumer, ConsumerConfig, MultiplePartitionConsumer, PartitionSelectionStrategy,
};
pub use offset::Offset;

pub use crate::admin::FluvioAdmin;
pub use crate::fluvio::Fluvio;

pub use fluvio_compression::Compression;

/// The minimum VERSION of the Fluvio Platform that this client is compatible with.
const MINIMUM_PLATFORM_VERSION: &str = "0.9.0";

/// Creates a producer that sends records to the named topic
///
/// This is a shortcut function that uses the current profile
/// settings. If you need to specify any custom configurations,
/// try directly creating a [`Fluvio`] client object instead.
///
/// # Example: Simple records
///
/// Fluvio can send "simple" records that contain arbitrary
/// binary data. An easy way to demonstrate this is by sending
/// a string:
///
/// ```no_run
/// # use fluvio::{FluvioError, RecordKey};
/// # async fn do_produce() -> Result<(), FluvioError> {
/// let producer = fluvio::producer("my-topic").await?;
/// producer.send(RecordKey::NULL, "Hello, world!").await?;
/// # Ok(())
/// # }
/// ```
///
/// # Example: Key-value records
///
/// Fluvio also supports "key-value" records, where the key and
/// the value given may each be any binary data. Typically, the
/// key represents some unique property of the value, such as
/// an email address, username, or request ID.
///
/// ```no_run
/// # use fluvio::FluvioError;
/// # async fn do_produce() -> Result<(), FluvioError> {
/// let producer = fluvio::producer("my-topic").await?;
/// let key = "fluvio";
/// let value = r#"
/// {"project":"fluvio","about":"Data streaming in Rust!"}
/// "#;
/// producer.send(key, value).await?;
/// # Ok(())
/// # }
/// ```
///
/// [`Fluvio`]: ./struct.Fluvio.html
#[instrument(skip(topic))]
pub async fn producer<S: Into<String>>(topic: S) -> Result<TopicProducer, FluvioError> {
    let fluvio = Fluvio::connect().await?;
    let producer = fluvio.topic_producer(topic).await?;
    Ok(producer)
}

/// Creates a producer that receives events from the given topic and partition
///
/// This is a shortcut function that uses the current profile
/// settings. If you need to specify any custom configurations,
/// try directly creating a [`Fluvio`] client object instead.
///
/// # Example
///
/// ```no_run
/// # use fluvio::{ConsumerConfig, FluvioError, Offset};
/// # mod futures {
/// #     pub use futures_util::stream::StreamExt;
/// # }
/// #  async fn example() -> Result<(), FluvioError> {
/// use futures::StreamExt;
/// let consumer = fluvio::consumer("my-topic", 0).await?;
/// let mut stream = consumer.stream(Offset::beginning()).await?;
/// while let Some(Ok(record)) = stream.next().await {
///     let key = record.key().map(|key| String::from_utf8_lossy(key).to_string());
///     let value = String::from_utf8_lossy(record.value()).to_string();
///     println!("Got record: key={:?}, value={}", key, value);
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`Fluvio`]: ./struct.Fluvio.html
#[instrument(skip(topic, partition))]
pub async fn consumer<S: Into<String>>(
    topic: S,
    partition: i32,
) -> Result<PartitionConsumer, FluvioError> {
    let fluvio = Fluvio::connect().await?;
    let consumer = fluvio.partition_consumer(topic, partition).await?;
    Ok(consumer)
}

/// re-export metadata from sc-api
pub mod metadata {

    pub use fluvio_sc_schema::AdminSpec;

    pub mod topic {
        pub use fluvio_sc_schema::topic::*;
    }

    pub mod connector {
        pub use fluvio_sc_schema::connector::*;
    }

    pub mod smartmodule {
        pub use fluvio_sc_schema::smartmodule::*;
    }

    pub mod derivedstream {
        pub use fluvio_sc_schema::derivedstream::*;
    }

    pub mod customspu {
        pub use fluvio_sc_schema::customspu::*;
    }

    pub mod spu {
        pub use fluvio_sc_schema::spu::*;
    }

    pub mod spg {
        pub use fluvio_sc_schema::spg::*;
    }

    pub mod partition {
        pub use fluvio_sc_schema::partition::*;
    }

    pub mod objects {
        pub use fluvio_sc_schema::objects::*;
    }

    pub mod tableformat {
        pub use fluvio_sc_schema::tableformat::*;
    }

    pub mod core {
        pub use fluvio_sc_schema::core::*;
    }

    pub mod store {
        pub use fluvio_sc_schema::store::*;
    }
}

pub mod dataplane {
    pub use fluvio_protocol::*;
}
