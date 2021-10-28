use std::sync::Arc;

use fluvio_sc_schema::ObjectDecoder;
use fluvio_sc_schema::objects::ObjectApiWatchRequest;
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
    watch_version: i16,
}

impl MetadataStores {
    /// start synchronization

    #[instrument(skip(socket))]
    pub async fn start(
        socket: SharedMultiplexerSocket,
        watch_version: i16,
    ) -> Result<Self, SocketError> {
        debug!(watch_version, "starting metadata store");
        let store = Self {
            shutdown: SimpleEvent::shared(),
            spus: StoreContext::new(),
            partitions: StoreContext::new(),
            topics: StoreContext::new(),
            socket,
            watch_version,
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

        let mut req_msg = RequestMessage::new_request(WatchRequest::default());
        req_msg.get_mut_header().set_api_version(self.watch_version);

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

        let watch_request: WatchRequest<PartitionSpec> = WatchRequest::default();
        let (watch_req, mw): (ObjectApiWatchRequest, ObjectDecoder) = watch_request.into();
        let req_msg = RequestMessage::request_with_mw(watch_req, mw);
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

        let req_msg = RequestMessage::new_request(WatchRequest::default());
        let async_response = self.socket.create_stream(req_msg, 10).await?;

        MetadataSyncController::<TopicSpec>::start(
            self.topics.clone(),
            async_response,
            self.shutdown.clone(),
        );

        Ok(())
    }
}
