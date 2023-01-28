use anyhow::Result;

use async_trait::async_trait;

use fluvio::Offset;
use fluvio_connector_common::{Sink, LocalBoxSink};

use crate::CustomConfig;

#[derive(Debug)]
pub(crate) struct TestSink {}

impl TestSink {
    pub(crate) fn new(_config: &CustomConfig) -> Result<Self> {
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
