use std::{
    time::Duration,
    task::{Poll, Context},
    pin::Pin,
};

use anyhow::Result;

use async_trait::async_trait;
use fluvio::Offset;
use fluvio_connector_common::{Source, ConnectorConfig};
use futures::{stream::LocalBoxStream, Stream, StreamExt};

use tokio::time::Interval;

#[derive(Debug)]
pub(crate) struct TestJsonSource {
    interval: Interval,
    template: String,
}

impl TestJsonSource {
    pub(crate) fn new(config: &ConnectorConfig) -> Result<Self> {
        let interval = match config.parameters.get("interval") {
            Some(value) => value.as_u32()?,
            None => anyhow::bail!("interval not found"),
        };
        let template = match config.parameters.get("template") {
            Some(value) => value.as_string()?,
            None => anyhow::bail!("template not found"),
        };
        Ok(Self {
            interval: tokio::time::interval(Duration::from_secs(interval as u64)),
            template,
        })
    }
}

impl Stream for TestJsonSource {
    type Item = String;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.interval
            .poll_tick(cx)
            .map(|_| Some(self.template.clone()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (std::usize::MAX, None)
    }
}

#[async_trait]
impl<'a> Source<'a, String> for TestJsonSource {
    async fn connect(self, _offset: Option<Offset>) -> Result<LocalBoxStream<'a, String>> {
        Ok(self.boxed_local())
    }
}
