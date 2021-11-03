
use tracing::debug;
use tracing::instrument;

use fluvio_future::task::spawn;

use crate::core::SharedContext;
use crate::stores::smartstream::SmartStreamSpec;
use crate::stores::{StoreContext, K8ChangeListener};

pub struct SmartStreamController {
    smartstreams: StoreContext<SmartStreamSpec>
    
}

impl SmartStreamController {

    pub fn start(ctx: SharedContext) {

        let smartstreams = ctx.smartstreams().clone();

        let controller = Self {
            smartstreams
        };
        
        spawn(controller.dispatch_loop());
    }

    #[instrument(name = "SmartStreamController", skip(self))]
    async fn dispatch_loop(mut self) {
        use std::time::Duration;
    }

}