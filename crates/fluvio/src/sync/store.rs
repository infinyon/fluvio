use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt::Display;
use std::sync::Arc;

use tracing::{debug, instrument};
use anyhow::Result;

use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;
use fluvio_sc_schema::AdminSpec;
use fluvio_sc_schema::objects::Metadata;
use fluvio_sc_schema::objects::ObjectApiWatchRequest;
use fluvio_sc_schema::objects::ObjectApiWatchResponse;
use fluvio_sc_schema::objects::WatchRequest;
use fluvio_sc_schema::objects::WatchResponse;
use fluvio_socket::AsyncResponse;
use fluvio_socket::SharedMultiplexerSocket;

use crate::metadata::topic::TopicSpec;
use crate::metadata::spu::SpuSpec;
use crate::metadata::partition::PartitionSpec;

use super::CacheMetadataStoreObject;
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
    pub async fn start(socket: SharedMultiplexerSocket, watch_version: i16) -> Result<Self> {
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
    pub async fn start_watch_for_spu(&self) -> Result<()> {
        self.start_watch::<SpuSpec>(self.spus.clone()).await?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn start_watch_for_partition(&self) -> Result<()> {
        self.start_watch::<PartitionSpec>(self.partitions.clone())
            .await?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn start_watch_for_topic(&self) -> Result<()> {
        self.start_watch::<TopicSpec>(self.topics.clone()).await?;

        Ok(())
    }

    #[instrument(skip(self, store))]
    async fn start_watch<S>(&self, store: StoreContext<S>) -> Result<()>
    // same bounds as MetadataSyncController
    where
        S: AdminSpec + 'static + Sync + Send,
        ObjectApiWatchRequest: From<WatchRequest<S>>,
        AsyncResponse<ObjectApiWatchRequest>: Send,
        S: Encoder + Decoder + Send + Sync,
        S::Status: Sync + Send + Encoder + Decoder,
        S::IndexKey: Display + Sync + Send,
        <WatchResponse<S> as TryFrom<ObjectApiWatchResponse>>::Error: Display + Send,
        CacheMetadataStoreObject<S>: TryFrom<Metadata<S>>,
        WatchResponse<S>: TryFrom<ObjectApiWatchResponse>,
        <Metadata<S> as TryInto<CacheMetadataStoreObject<S>>>::Error: Display,
    {
        use fluvio_protocol::api::RequestMessage;
        use fluvio_sc_schema::objects::WatchRequest;

        let watch_request: WatchRequest<S> = WatchRequest::default();
        let watch_req: ObjectApiWatchRequest = watch_request.into();
        let mut req_msg = RequestMessage::new_request(watch_req);
        req_msg.get_mut_header().set_api_version(self.watch_version);

        debug!(watch_version = self.watch_version, obj = %S::LABEL, "create metadata stream");
        let async_response = self.socket.create_stream(req_msg, 10).await?;

        MetadataSyncController::<S>::start(store, async_response, self.shutdown.clone());

        Ok(())
    }
}
