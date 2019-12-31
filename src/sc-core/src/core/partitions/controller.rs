//!
//! # Auth Controller
//!

use log::trace;
use log::error;
use log::info;
use futures::select;
use futures::stream::StreamExt;
use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::Sender;
use futures::channel::mpsc::channel;

use types::log_on_err;
use flv_metadata::partition::PartitionSpec;
use flv_metadata::spu::SpuSpec;
use flv_future_core::spawn;
use internal_api::UpdateLrsRequest;

use crate::core::WSUpdateService;
use crate::core::ShareLocalStores;
use crate::conn_manager::SharedConnManager;

use crate::core::WSChangeChannel;

use super::PartitionReducer;
use super::PartitionChangeRequest;

#[derive(Debug)]
pub struct PartitionController<W> {
    local_stores: ShareLocalStores,
    conn_manager: SharedConnManager,
    ws_service: W,
    partition_receiver: WSChangeChannel<PartitionSpec>,
    spu_receiver: WSChangeChannel<SpuSpec>,
    lrs_receiver: Receiver<UpdateLrsRequest>,
    lrs_sender: Sender<UpdateLrsRequest>,
    reducer: PartitionReducer,
}

impl<W> PartitionController<W>
where
    W: WSUpdateService + Send + Sync + 'static,
{
    pub fn new(
        local_stores: ShareLocalStores,
        conn_manager: SharedConnManager,
        partition_receiver: WSChangeChannel<PartitionSpec>,
        spu_receiver: WSChangeChannel<SpuSpec>,
        ws_service: W,
    ) -> Self {
        let (lrs_sender, lrs_receiver) = channel(100);

        Self {
            ws_service,
            conn_manager,
            local_stores: local_stores.clone(),
            reducer: PartitionReducer::new(
                local_stores.partitions().clone(),
                local_stores.spus().clone(),
            ),
            spu_receiver,
            partition_receiver,
            lrs_receiver,
            lrs_sender,
        }
    }

    pub fn lrs_sender(&self) -> Sender<UpdateLrsRequest> {
        self.lrs_sender.clone()
    }

    pub fn run(self) {
        spawn(self.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        loop {
            select! {
                partition_req = self.partition_receiver.next() => {
                    match partition_req {
                        None => {
                            error!("Partition LC dispatcher has been terminated.  Ending Server loop");
                            break;
                        },
                        Some(request) => {
                            trace!("receive partition request  {:#?}",request);
                            self.process_request(PartitionChangeRequest::Partition(request)).await;
                        },
                    }
                },
                spu_req = self.spu_receiver.next() => {
                    match spu_req {
                        None => {
                            error!("SPU lC dispatcher has been terminated. Ending server loop");
                            break;
                        },
                        Some(request) => {
                            trace!("received SPU request {:#?}",request);
                            self.process_request(PartitionChangeRequest::Spu(request)).await;
                        }
                    }
                },
                lrs_req = self.lrs_receiver.next() => {
                    match lrs_req {
                        None => {
                            error!("LRS channel has been terminated.  Ending server loop");
                            break;
                        },
                        Some(req) => {
                            trace!("Lrs status request: {:#?}",req);
                            self.process_request(PartitionChangeRequest::LrsUpdate(req)).await;
                        }
                    }
                }
                complete => {},
            }
        }

        info!("spu controller is terminated");
    }

    async fn process_request(&mut self, requests: PartitionChangeRequest) {
        // process partition actions; generate Kvs actions and SPU msgs.
        match self.reducer.process_requests(requests) {
            Ok(actions) => {
                trace!("Partition actions: {}", actions);

                if actions.partitions.count() > 0 {
                    for ws_action in actions.partitions.into_iter() {
                        log_on_err!(self.ws_service.update_partition(ws_action).await);
                    }
                }

                if actions.conns.count() > 0 {
                    self.conn_manager.process_requests(actions.conns).await;
                }
            }
            Err(err) => error!("error processing partition requests: {}", err),
        }
    }
}
