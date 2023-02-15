use anyhow::Result;

use async_trait::async_trait;

use fluvio::Offset;
use fluvio_connector_common::{Sink, LocalBoxSink, tracing::debug};

use crate::CustomConfig;

#[derive(Debug)]
pub(crate) struct TestSink {}

impl TestSink {
    pub(crate) fn new(config: &CustomConfig) -> Result<Self> {
        debug!(?config.api_key);
        let resolved = config.api_key.resolve()?;
        debug!(resolved);
        Ok(Self {})
    }
}

#[async_trait]
impl Sink<String> for TestSink {
    async fn connect(self, _offset: Option<Offset>) -> Result<LocalBoxSink<String>> {
        let unfold = futures::sink::unfold((), |_: (), record: String| async move {
            println!("Received record: {record}");
            Ok::<_, anyhow::Error>(())
        });
        Ok(Box::pin(unfold))
    }
}
