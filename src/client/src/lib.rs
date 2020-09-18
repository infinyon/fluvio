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
//! # Example
//!
//! The easiest way to create a Fluvio Topic is by using the [Fluvio CLI].
//!
//! ```bash
//! $ fluvio topic create echo
//! topic "echo" created
//! ```
//!
//! To produce some records, you'll need to initialize the Fluvio client
//!
//! ```no_run
//! # use fluvio::{Fluvio, FluvioError};
//! # async fn do_init_fluvio() -> Result<(), FluvioError> {
//! let fluvio = Fluvio::connect().await?;
//! # }
//! ```
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
pub use producer::PartitionProducer;
pub use consumer::PartitionConsumer;

pub use crate::admin::FluvioAdmin;
pub use crate::client::Fluvio;

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
