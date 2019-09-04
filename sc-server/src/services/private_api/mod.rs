mod private_server;
mod internal_context;

pub use internal_context::InternalContext;

use std::sync::Arc;

use log::info;
use futures::channel::mpsc::Sender;

use private_server::ScInternalService;
use internal_api::InternalScKey;
use internal_api::InternalScRequest;
use internal_api::UpdateLrsRequest;
use kf_service::KfApiServer;

use crate::core::ShareLocalStores;
use crate::conn_manager::SharedConnManager;
use crate::conn_manager::SpuConnectionStatusChange;

pub type SharedInternalContext = Arc<InternalContext>;

pub type InternalApiServer = KfApiServer<InternalScRequest, InternalScKey, SharedInternalContext, ScInternalService>;

// start server
pub fn create_internal_server(
    local_stores: ShareLocalStores,
    conn_mgr: SharedConnManager,
    conn_status_sender: Sender<SpuConnectionStatusChange>,
     lrs_sender: Sender<UpdateLrsRequest>,
) -> InternalApiServer
{
    let addr = local_stores.config().private_endpoint.addr.clone();
    let ctx = InternalContext::new(
        local_stores,
        conn_mgr,
        conn_status_sender,
        lrs_sender
    );
    info!("SC: starting internal services at: {}", addr);

    KfApiServer::new(addr, Arc::new(ctx), ScInternalService::new())
}
