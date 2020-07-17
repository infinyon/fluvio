use log::debug;

use flv_future_aio::task::spawn;
use super::SharedMetadataStore;

/// sync from sc
pub struct MetadataController {

    metadata: SharedMetadataStore
}


impl MetadataController {

    pub fn new(metadata: SharedMetadataStore) -> Self {
        Self {
            metadata
        }
    }

    pub fn run(self) {
        spawn(self.dispatch_loop());
    }   


    async fn dispatch_loop(mut self) {

        debug!("starting dispatch loop");

        

    }
    
}