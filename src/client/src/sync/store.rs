use std::sync::Arc;

use tracing::debug;

#[cfg(not(target_arch = "wasm32"))]
use fluvio_socket::AllMultiplexerSocket as FluvioMultiplexerSocket;

#[cfg(target_arch = "wasm32")]
use crate::websocket::MultiplexerWebsocket as FluvioMultiplexerSocket;

use crate::metadata::topic::TopicSpec;
use crate::FluvioError;
use crate::metadata::spu::SpuSpec;
use crate::metadata::partition::PartitionSpec;

use super::controller::{MetadataSyncController, SimpleEvent};
use super::StoreContext;

#[derive(Clone)]
/// global cached stores necessary for consumer and producers
pub struct MetadataStores {
    shutdown: Arc<SimpleEvent>,
    spus: StoreContext<SpuSpec>,
    partitions: StoreContext<PartitionSpec>,
    topics: StoreContext<TopicSpec>,
    socket: Arc<FluvioMultiplexerSocket>,
}

impl MetadataStores {
    /// start synchronization
    pub async fn start(socket: Arc<FluvioMultiplexerSocket>) -> Result<Self, FluvioError> {
        let store = Self {
            shutdown: SimpleEvent::shared(),
            spus: StoreContext::new(),
            partitions: StoreContext::new(),
            topics: StoreContext::new(),
            socket,
        };

        store.start_watch_for_spu().await?;
        store.start_watch_for_partition().await?;
        store.start_watch_for_topic().await?;

        Ok(store)
    }

    pub fn spus(&self) -> &StoreContext<SpuSpec> {
        &self.spus
    }

    pub fn partitions(&self) -> &StoreContext<PartitionSpec> {
        &self.partitions
    }

    pub fn topics(&self) -> &StoreContext<TopicSpec> {
        &self.topics
    }

    pub fn shutdown(&mut self) {
        self.shutdown.notify();
    }

    /// start watch for spu
    pub async fn start_watch_for_spu(&self) -> Result<(), FluvioError> {
        use dataplane::api::RequestMessage;
        use fluvio_sc_schema::objects::WatchRequest;

        debug!("start watch for spu");

        let req_msg = RequestMessage::new_request(WatchRequest::Spu(0));
        let async_response = self.socket.create_stream(req_msg, 10).await?;

        MetadataSyncController::<SpuSpec>::start(
            self.spus.clone(),
            async_response,
            self.shutdown.clone(),
        );

        Ok(())
    }

    pub async fn start_watch_for_partition(&self) -> Result<(), FluvioError> {
        use dataplane::api::RequestMessage;
        use fluvio_sc_schema::objects::WatchRequest;

        debug!("start watch for partition");

        let req_msg = RequestMessage::new_request(WatchRequest::Partition(0));
        let async_response = self.socket.create_stream(req_msg, 10).await?;

        MetadataSyncController::<PartitionSpec>::start(
            self.partitions.clone(),
            async_response,
            self.shutdown.clone(),
        );

        Ok(())
    }

    pub async fn start_watch_for_topic(&self) -> Result<(), FluvioError> {
        use dataplane::api::RequestMessage;
        use fluvio_sc_schema::objects::WatchRequest;

        debug!("start watch for topic");

        let req_msg = RequestMessage::new_request(WatchRequest::Topic(0));
        let async_response = self.socket.create_stream(req_msg, 10).await?;

        MetadataSyncController::<TopicSpec>::start(
            self.topics.clone(),
            async_response,
            self.shutdown.clone(),
        );

        Ok(())
    }
}
