
use log::error;
use log::debug;
use futures::channel::mpsc::Sender;
use futures::sink::SinkExt;


use flv_types::SpuId;
use kf_socket::KfSink;
use internal_api::UpdateLrsRequest;

use crate::core::ShareLocalStores;
use crate::conn_manager::SharedConnManager;
use crate::conn_manager::ConnParams;
use crate::conn_manager::SpuConnectionStatusChange;

/// Context used by Private API Server
pub struct InternalContext
{
    pub local_stores: ShareLocalStores,
    conn_mgr: SharedConnManager,
    conn_status_sender: Sender<SpuConnectionStatusChange>,
    lrs_sender: Sender<UpdateLrsRequest>
}


impl InternalContext {

    pub fn new(
        local_stores: ShareLocalStores,
        conn_mgr: SharedConnManager,
        conn_status_sender: Sender<SpuConnectionStatusChange>,
        lrs_sender: Sender<UpdateLrsRequest>
    ) -> Self {
            Self {
                local_stores,
                conn_mgr,
                conn_status_sender,
                lrs_sender
            }
        }

    /// send connection status to all receivers
    async fn send_state_to_sender(&self,state: SpuConnectionStatusChange) {
        
        let mut sender = self.conn_status_sender.clone();
        if let Err(err) = sender.send(state).await {
            error!("error sending connection state to sender: {:#?}",err);
        }
    }

    pub async fn send_lrs_to_sender(&self, lrs: UpdateLrsRequest) {
        let mut sender = self.lrs_sender.clone();
        if let Err(err) = sender.send(lrs).await {
            error!("error sending lrs state to sender: {:#?}",err);
        }
    }

    /// Register new sink 
    /// true if successfully register
    pub async fn register_sink(&self, spu_id: SpuId, sink: KfSink, param: ConnParams)  {

        self.conn_mgr.register_sink(spu_id,sink,param).await;
        debug!("Successfully registered SPU {}",spu_id);
        self.send_state_to_sender(SpuConnectionStatusChange::On(spu_id)).await;
    }

     /// Unregist sink.  This happens when connection when down
    pub async fn clear_sink(&self,spu_id: &SpuId) {
        self.conn_mgr.clear_sink(spu_id).await;
        debug!("removing socket sink for spu: {}",spu_id);
        self.send_state_to_sender(SpuConnectionStatusChange::Off(*spu_id)).await;
    }

    pub fn validate_spu(&self, spu_id: &SpuId) -> bool {
        self.conn_mgr.validate_spu(spu_id)
    }

}