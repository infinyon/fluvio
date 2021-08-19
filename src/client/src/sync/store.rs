use std::sync::Arc;

use tracing::{debug, instrument};

use fluvio_socket::SharedMultiplexerSocket;
use fluvio_socket::SocketError;

use crate::metadata::topic::TopicSpec;
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
    socket: SharedMultiplexerSocket,
}

impl MetadataStores {
    /// start synchronization

    #[instrument()]
    pub async fn start(socket: SharedMultiplexerSocket) -> Result<Self, SocketError> {
        debug!("starting metadata store");
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
    #[instrument(skip(self))]
    pub async fn start_watch_for_spu(&self) -> Result<(), SocketError> {
        use dataplane::api::RequestMessage;
        use fluvio_sc_schema::objects::WatchRequest;

        let req_msg = RequestMessage::new_request(WatchRequest::Spu(0));
        debug!("create spu metadata stream");
        let async_response = self.socket.create_stream(req_msg, 10).await?;

        MetadataSyncController::<SpuSpec>::start(
            self.spus.clone(),
            async_response,
            self.shutdown.clone(),
        );

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn start_watch_for_partition(&self) -> Result<(), SocketError> {
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

    #[instrument(skip(self))]
    pub async fn start_watch_for_topic(&self) -> Result<(), SocketError> {
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
