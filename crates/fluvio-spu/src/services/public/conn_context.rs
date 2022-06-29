use crate::services::public::StreamPublishers;

#[derive(Debug)]
pub(crate) struct ConnectionContext {
    stream_publishers: StreamPublishers,
}

impl ConnectionContext {
    pub(crate) fn new() -> Self {
        Self {
            stream_publishers: StreamPublishers::new(),
        }
    }

    pub(crate) fn stream_publishers(&self) -> &StreamPublishers {
        &self.stream_publishers
    }

    pub(crate) fn stream_publishers_mut(&mut self) -> &mut StreamPublishers {
        &mut self.stream_publishers
    }
}
