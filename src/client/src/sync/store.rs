use std::sync::Arc;

use tracing::debug;

use fluvio_socket::AllMultiplexerSocket;
use fluvio_socket::FlvSocketError;

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
}

impl MetadataStores {

    /// start synchronization
    pub async fn start(socket: &AllMultiplexerSocket) -> Result<Self, FlvSocketError> {
        let store = Self {
            shutdown: SimpleEvent::shared(),
            spus: StoreContext::new(),
            partitions: StoreContext::new(),
        };

        store.start_watch_for_spu(socket).await?;
        store.start_watch_for_partition(socket).await?;

        Ok(store)
    }



    pub fn spus(&self) -> &StoreContext<SpuSpec> {
        &self.spus
    }

    pub fn partitions(&self) -> &StoreContext<PartitionSpec> {
        &self.partitions
    }

    pub fn shutdown(&mut self) {
        self.shutdown.notify();
    }

    /// start watch for spu
    pub async fn start_watch_for_spu(
        &self,
        socket: &AllMultiplexerSocket,
    ) -> Result<(), FlvSocketError> {
        use dataplane::api::RequestMessage;
        use fluvio_sc_schema::objects::WatchRequest;

        debug!("start watch for spu");

        let req_msg = RequestMessage::new_request(WatchRequest::Spu(0));
        let async_response = socket.create_stream(req_msg, 10).await?;

        MetadataSyncController::<SpuSpec>::start(
            self.spus.clone(),
            async_response,
            self.shutdown.clone(),
        );

        Ok(())
    }

    pub async fn start_watch_for_partition(
        &self,
        socket: &AllMultiplexerSocket,
    ) -> Result<(), FlvSocketError> {
        use dataplane::api::RequestMessage;
        use fluvio_sc_schema::objects::WatchRequest;

        debug!("start watch for partition");

        let req_msg = RequestMessage::new_request(WatchRequest::Partition(0));
        let async_response = socket.create_stream(req_msg, 10).await?;

        MetadataSyncController::<PartitionSpec>::start(
            self.partitions.clone(),
            async_response,
            self.shutdown.clone(),
        );

        Ok(())
    }
}
