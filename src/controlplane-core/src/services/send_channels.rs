//!
//! # Dispatcher Send Channels
//!
//! Stores the sender part of the channel used by the Dispathers
//!
use futures::channel::mpsc::Sender;
use futures::sink::SinkExt;
use flv_types::log_on_err;
use utils::actions::Actions;

use crate::core::ScRequest;
use crate::core::spus::SpuAction;
use crate::core::partitions::PartitionAction;


/// Central action dispatcher
/// It will send action to appropriate controller.
/// For now, it will send to a central controller, but in the future
/// it will send to individual controller
#[derive(Debug, Clone)]
pub struct DispatcherSendChannels(Sender<ScRequest>);

impl DispatcherSendChannels {
    pub fn new(sc_sender: Sender<ScRequest>) -> Self {
        Self(sc_sender)
    }

    pub async fn send_msg_to_sc(&mut self, request: ScRequest) {
        log_on_err!(
            self.0.send(request).await,
            "send Dispatch req to SC: {}"
        );
    }

   

    /// send spu actions to spu controller
    pub async fn send_spu_actions(&mut self,actions: Actions<SpuAction>) {
        //self.send_msg_to_sc(ScRequest::UpdateSPUs(actions)).await;
    }


    pub async fn send_partition_actions(&mut self,actions: Actions<PartitionAction>) {
       // self.send_msg_to_sc(ScRequest::UpdatePartitions(actions)).await;
    }    

}
