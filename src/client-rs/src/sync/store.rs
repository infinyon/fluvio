use log::debug;

use kf_socket::AllMultiplexerSocket;
use kf_socket::KfSocketError;

use crate::metadata::spu::SpuSpec;
use crate::metadata::partition::PartitionSpec;

use super::controller::MetadataSyncController;
use super::StoreContext;

#[derive(Clone)]
/// global cached stores necessary for consumer and producers
pub struct MetadataStores {
    spus: StoreContext<SpuSpec>,
    partitions: StoreContext<PartitionSpec>,
}

impl MetadataStores {
    /// crete store and set up sync controllers
    pub async fn new(socket: &mut AllMultiplexerSocket) -> Result<Self, KfSocketError> {
        let store = Self {
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

    /// start watch for spu
    pub async fn start_watch_for_spu(
        &self,
        socket: &mut AllMultiplexerSocket,
    ) -> Result<(), KfSocketError> {
        use kf_protocol::api::RequestMessage;
        use flv_api_sc::objects::WatchRequest;

        debug!("start watch for spu");

        let req_msg = RequestMessage::new_request(WatchRequest::Spu(0));
        let async_response = socket.send_with_async_response(req_msg, 10).await?;

        MetadataSyncController::<SpuSpec>::start(self.spus.clone(), async_response);

        Ok(())
    }

    pub async fn start_watch_for_partition(
        &self,
        socket: &mut AllMultiplexerSocket,
    ) -> Result<(), KfSocketError> {
        use kf_protocol::api::RequestMessage;
        use flv_api_sc::objects::WatchRequest;

        debug!("start watch for partition");

        let req_msg = RequestMessage::new_request(WatchRequest::Partition(0));
        let async_response = socket.send_with_async_response(req_msg, 10).await?;

        MetadataSyncController::<PartitionSpec>::start(self.partitions.clone(), async_response);

        Ok(())
    }
}
