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
//! use std::time::Duration;
//! use fluvio::{Fluvio, FluvioError};
//! use fluvio::params::{FetchOffset, FetchLogOption};
//!
//! async_std::task::spawn(produce_records());
//! if let Err(e) = async_std::task::block_on(consume_records()) {
//!     println!("Error: {}", e);
//! }
//!
//! async fn produce_records() -> Result<(), FluvioError> {
//!     let producer = fluvio::producer("echo").await?;
//!     for i in 0..10u8 {
//!         producer.send_record(format!("Hello, Fluvio {}!", i), 0).await?;
//!         async_std::task::sleep(Duration::from_secs(1)).await;
//!     }
//!     Ok(())
//! }
//!
//! async fn consume_records() -> Result<(), FluvioError> {
//!     let consumer = fluvio::consumer("echo", 0).await?;
//!     let offset = FetchOffset::Earliest(None);
//!     let fetch_config = FetchLogOption::default();
//!     let mut stream = consumer.stream(offset, fetch_config).await?;
//!
//!     while let Ok(event) = stream.next().await {
//!         for batch in event.partition.records.batches {
//!             for record in batch.records {
//!                 if let Some(record) = record.value.inner_value() {
//!                     let string = String::from_utf8(record)
//!                         .expect("record should be a string");
//!                     println!("Got record: {}", string);
//!                 }
//!             }
//!         }
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
mod client;
mod admin;
mod consumer;
mod producer;
mod sync;
mod spu;

pub mod config;
pub mod params;

pub use error::FluvioError;
pub use config::FluvioConfig;
pub use producer::TopicProducer;
pub use consumer::PartitionConsumer;

pub use crate::admin::FluvioAdmin;
pub use crate::client::Fluvio;

/// Creates a producer that sends events to the named topic
///
/// This is a shortcut function that uses the current profile
/// settings. If you need to specify any custom configurations,
/// try directly creating a [`Fluvio`] client object instead.
///
/// # Example
///
/// ```no_run
/// # use fluvio::FluvioError;
/// # async fn do_produce() -> Result<(), FluvioError> {
/// let producer = fluvio::producer("my-topic").await?;
/// producer.send_record("Hello, world!", 0).await?;
/// # Ok(())
/// # }
/// ```
///
/// [`Fluvio`]: ./struct.Fluvio.html
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
/// # use fluvio::FluvioError;
/// use fluvio::params::{FetchOffset, FetchLogOption};
/// #  async fn do_consume() -> Result<(), FluvioError> {
/// let consumer = fluvio::consumer("my-topic", 0).await?;
/// let mut stream = consumer.stream(FetchOffset::Earliest(None), FetchLogOption::default()).await?;
/// while let Ok(event) = stream.next().await {
///     for batch in event.partition.records.batches {
///         for record in batch.records {
///             if let Some(record) = record.value.inner_value() {
///                 let string = String::from_utf8(record)
///                     .expect("record should be a string");
///                 println!("Got record: {}", string);
///             }
///         }
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`Fluvio`]: ./struct.Fluvio.html
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

    pub mod topic {
        pub use fluvio_sc_schema::topic::*;
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

    pub mod core {
        pub use fluvio_sc_schema::core::*;
    }

    pub mod store {
        pub use fluvio_sc_schema::store::*;
    }
}

pub mod dataplane {
    pub use dataplane::*;
}
