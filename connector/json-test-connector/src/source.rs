use std::{
    time::Duration,
    task::{Poll, Context},
    pin::Pin,
};

use anyhow::Result;

use async_trait::async_trait;
use fluvio::Offset;
use fluvio_connector_common::Source;
use futures::{stream::LocalBoxStream, Stream, StreamExt};

use tokio::time::Interval;

use crate::CustomConfig;

#[derive(Debug)]
pub(crate) struct TestJsonSource {
    interval: Interval,
    template: String,
}

impl TestJsonSource {
    pub(crate) fn new(config: &CustomConfig) -> Result<Self> {
        let CustomConfig { interval, template } = config;
        Ok(Self {
            interval: tokio::time::interval(Duration::from_secs(*interval)),
            template: template.clone(),
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
