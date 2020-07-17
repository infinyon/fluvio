use std::sync::Arc;

use log::debug;
use log::trace;

use kf_socket::AllMultiplexerSocket;
use flv_api_sc::spu::store::DefaultSpuStore;
use flv_api_sc::partition::store::DefaultPartitionStore;

use crate::ClientError;

pub type SharedMetadataStore = Arc<MetadataStores>;

pub struct MetadataStores {

    spus: Arc<DefaultSpuStore>,
    partitions: Arc<DefaultPartitionStore>

}

impl MetadataStores {

    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    fn new() -> Self {
        Self {
            spus: DefaultSpuStore::new_shared(),
            partitions: DefaultPartitionStore::new_shared()
        }
    }

    pub fn spus(&self) -> &DefaultSpuStore {
        &self.spus
    }

    pub fn partitions(&self) -> &DefaultPartitionStore {
        &self.partitions
    }


    pub fn buld_add() {
        
    }

    /// start watch on metadata
    pub async fn start_metadata_watch(
        &self,
        socket: &mut AllMultiplexerSocket,

    ) -> Result<(), ClientError> {

        use std::time::Duration;

        use kf_protocol::api::RequestMessage;
        use flv_api_sc::metadata::WatchMetadataRequest;

        debug!("sending start metadata watch");

        let req_msg = RequestMessage::new_request(WatchMetadataRequest::default());

        let mut metadata_async_response = socket.send_with_async_response(req_msg, 10).await?;        

        let full_metadata = metadata_async_response.next_timeout(Duration::from_secs(60)).await?;

        

        debug!("receives  metadata: {}",full_metadata);

        trace!("metadata: {:#?}",full_metadata);
        
        Ok(())
        
    }
}