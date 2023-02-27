pub mod producer;
pub mod smartmodule;
pub mod monitoring;
pub mod consumer;
pub mod config;

pub use fluvio_connector_package::secret;

#[cfg(feature = "derive")]
pub use fluvio_connector_derive::connector;

use fluvio::{Offset, metadata::topic::TopicSpec};
use futures::stream::LocalBoxStream;
use async_trait::async_trait;

pub type Error = anyhow::Error;
pub type Result<T> = std::result::Result<T, Error>;

pub mod future {
    pub use fluvio_future::task::run_block_on;
    pub use fluvio_future::subscriber::init_logger;
    pub use fluvio_future::retry;
}

pub mod tracing {
    pub use ::tracing::*;
}

#[async_trait]
pub trait Source<'a, I> {
    async fn connect(self, offset: Option<Offset>) -> Result<LocalBoxStream<'a, I>>;
}

pub type LocalBoxSink<I> = std::pin::Pin<Box<dyn futures::Sink<I, Error = anyhow::Error>>>;

#[async_trait]
pub trait Sink<I> {
    async fn connect(self, offset: Option<Offset>) -> Result<LocalBoxSink<I>>;
}

pub async fn ensure_topic_exists(config: &config::ConnectorConfig) -> Result<()> {
    let admin = fluvio::FluvioAdmin::connect().await?;
    let topics = admin
        .list::<TopicSpec, String>(vec![config.meta.topic.clone()])
        .await?;
    let topic_exists = topics.iter().any(|t| t.name.eq(&config.meta.topic));
    if !topic_exists {
        let _ = admin
            .create(
                config.meta.topic.clone(),
                false,
                TopicSpec::new_computed(1, 1, Some(false)),
            )
            .await;
    }
    Ok(())
}
