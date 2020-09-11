//!
//! # Streaming Coordinator (SC) Dispatcher
//!
//! Receives actions from K8 dispatcher, identifies their action type and forwards them
//! to the processing corresponding processing engine.
//!

use std::io::Error as IoError;

use flv_future_core::spawn;
use futures::channel::mpsc::Receiver;
use futures::future::TryFutureExt;
use futures::select;
use futures::stream::StreamExt;
use tracing::{error, info};
use tracing::trace;

use crate::core::WSUpdateService;
use crate::conn_manager::SpuConnections;

use super::ScController;
use super::ScRequest;

/// Streaming Controller dispatcher entry point, spawns new thread
pub fn run<K,C>(receiver: Receiver<ScRequest>, sc_controller: ScController<K,C>) 
    where K: WSUpdateService + Send  + Sync + 'static,
        C: SpuConnections + Send + Sync + 'static
{
    info!("start SC[{}] dispatcher", sc_controller.id());

    spawn(sc_request_loop(receiver, sc_controller);
}

/// SC dispatcher request loop, waits for a request request and dispatchers
/// it for processing.
async fn sc_request_loop<K,C>(mut receiver: Receiver<ScRequest>, mut sc_controller: ScController<K,C>) 
    where K: WSUpdateService , C: SpuConnections  
{
    loop {
        select! {
            receiver_req = receiver.next() => {
                match receiver_req {
                    None => {
                        info!("SC dispatcher receiver is removed. end");
                        break;
                    },
                    Some(request) => {
                        trace!("SC Controller receive msg: {:#?}",request);
                        sc_controller.process_sc_request(request).await;
                    },
                }
            }
            complete => {},
        }
    }
}
