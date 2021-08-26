use std::sync::Arc;
use std::time::Duration;
use std::io::Error as IoError;

use tracing::info;
use tracing::debug;
use tracing::warn;
use tracing::trace;
use futures::select;
use futures::stream::StreamExt;
use futures::future::FutureExt;

use futures::channel::mpsc::Receiver;

use fluvio_controlplane::InternalSpuApi;
use fluvio_controlplane::InternalSpuRequest;
use fluvio_controlplane::RegisterSpuRequest;
use fluvio_controlplane::UpdateSpuRequest;
use fluvio_controlplane::UpdateReplicaRequest;
use fluvio_controlplane::messages::SpuContent;
use kf_protocol::api::RequestMessage;
use fluvio_socket::FlvSocket;
use fluvio_socket::FlvSocketError;
use fluvio_types::socket_helpers::EndPoint;
use fluvio_types::SpuId;
use utils::SimpleConcurrentHashMap;

use flv_future_core::spawn;
use flv_future_core::sleep;


use super::ScTestRunner;
use super::ScTest;

#[derive(Debug, Clone)]
pub struct SpuSpec {
    pub id: SpuId,
    pub end_point: EndPoint,
}

impl SpuSpec {
    pub fn new(id: SpuId, end_point: EndPoint) -> Self {
        Self { id, end_point }
    }
}


pub struct SpuGlobalContext {
    pub spec: SpuSpec,
    pub spus: SimpleConcurrentHashMap<String, SpuContent>
}

impl SpuGlobalContext {
    pub fn spec(&self) -> &SpuSpec {
        &self.spec
    }
}

pub type SharedSpuContext = Arc<SpuGlobalContext>;

impl SpuGlobalContext {
    pub fn new_shared_context(spec: SpuSpec) -> SharedSpuContext {
        Arc::new(SpuGlobalContext { 
            spec,
            spus: SimpleConcurrentHashMap::new()
        })
    }

    pub fn id(&self) -> SpuId {
        self.spec.id
    }

    pub fn run<T>(
        self: Arc<Self>,
        test_runner: Arc<ScTestRunner<T>>,
        receiver: Receiver<bool>,
    ) where
        T: ScTest + Sync + Send + 'static,
    {
        info!(
            "starting Mock SPU Server:{} at: {:#?}",
            self.spec.id, self.spec.end_point
        );

        MockSpuController::run(self.clone(), test_runner, receiver);

        // FlvApiServer::new(addr, self.clone(), MockInternalService::new(test_runner,self.clone()))
    }
}

/// Spu controller
pub struct MockSpuController<T> {
    receiver: Receiver<bool>,
    ctx: SharedSpuContext,
    test_runner: Arc<ScTestRunner<T>>,
}

impl<T> MockSpuController<T>
where
    T: ScTest + Sync + Send + 'static,
{
    /// start the controller with ctx and receiver
    pub fn run(ctx: SharedSpuContext, test_runner: Arc<ScTestRunner<T>>, receiver: Receiver<bool>) {
        let controller = Self {
            ctx,
            receiver,
            test_runner,
        };

        spawn(controller.inner_run());
    }

    async fn inner_run(mut self) -> Result<(), ()> {
        debug!("Mock spu: waiting 10ms to spin up");
        sleep(Duration::from_millis(10)).await.expect("panic");
        info!("starting SPU  Controller");

        loop {
            if let Some(socket) = self.create_socket_to_sc().await {
                trace!("established connection to  sc for spu: {}", self.ctx.id());
                match self.stream_loop(socket).await {
                    Ok(_) => break,
                    Err(err) => warn!("error, connecting to sc: {:#?}", err),
                }

                // 1 seconds is heuratic value, may change in the future or could be dynamic
                // depends on backoff algorithm
                sleep(Duration::from_millis(1000))
                    .await
                    .expect("waiting 5 seconds for each loop");
            }
        }

        Ok(())
    }

    /// process api stream from socket
    async fn stream_loop(&mut self, mut socket: FlvSocket) -> Result<(), FlvSocketError> {
        self.send_spu_registeration(&mut socket).await?;

        let (mut _sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<InternalSpuRequest, InternalSpuApi>();

        loop {
             select! {
                _ = self.receiver.next() =>  {
                    info!("spu: received termination msg");
                    break;
                },
                api_msg = api_stream.next().fuse() => {
                    if let Some(msg) = api_msg {
                        if let Ok(req_message) = msg {
                            tracing::trace!("received request: {:#?}",req_message);
                            match req_message {
                                InternalSpuRequest::UpdateSpuRequest(request) => {
                                    handle_spu_update_request(request, self.ctx.clone()).await.expect("spu handl should work");
                                },
                                InternalSpuRequest::UpdateReplicaRequest(request) => {
                                    handle_update_replica_request(request, self.ctx.clone()).await.expect("replica request");
                                }
                            }
                            
                        } else {
                            tracing::trace!("no content, end of connection {:#?}", msg);
                            break;
                        }

                    } else {
                        tracing::trace!("client connect terminated");
                        break;
                    }
                }
                
            }
        }

        info!("spu terminated");
        Ok(())
    
    }

    async fn send_spu_registeration<'a>(
        &'a self,
        socket: &'a mut FlvSocket,
    ) -> Result<(), FlvSocketError> {
        let spu_id = self.ctx.id();
        debug!("sending spu registeration: {}", spu_id);
        let mut message = RequestMessage::new_request(RegisterSpuRequest::new(spu_id));
        message
            .get_mut_header()
            .set_client_id(format!("spu: {}", spu_id));

        let _response = socket.send(&message).await?;
        debug!("received spu registeration: {}", spu_id);
        Ok(())
    }

    /// connect to sc if can't connect try until we succeed
    /// or if we received termination message
    async fn create_socket_to_sc(&mut self) -> Option<FlvSocket> {
        let spu_id = self.ctx.id();
        let sc_config = self.test_runner.test().env_configuration().sc_config();
        let addr = sc_config.private_endpoint.addr;
        let wait_interval = 10;
        loop {
            trace!(
                "trying to create socket to sc: {:#?} for spu: {}",
                addr,
                spu_id
            );
            let connect_future = FlvSocket::fusable_connect(&addr);

            select! {
                socket_res = connect_future.fuse() => {
                    match socket_res {
                        Ok(socket) => {
                            debug!("connected to sc for spu: {}",spu_id);
                            return Some(socket)
                        }
                        Err(err) => warn!("error connecting to sc: {}",err)
                    }

                    trace!("sleeping {} ms to connect to sc: {}",wait_interval,spu_id);
                    sleep(Duration::from_millis(wait_interval as u64)).await.expect("sleep should not fail");
                },
                _ = self.receiver.next() => {
                    info!("termination message received");
                    return None
                }
            }
        }
    }
}

async fn handle_spu_update_request(
    req_msg: RequestMessage<UpdateSpuRequest>,
    ctx: SharedSpuContext,
) -> Result<(), IoError> {
    let (_header, request) = req_msg.get_header_request();

    let req = request.content();

    debug!("spu update request: {:#?}", req);
    assert_eq!(req.target_spu, ctx.id());

    for msg in req.content.spus  {
        let mut spu_lock = ctx.spus.write();
        let spu_content = msg.content;
        spu_lock.insert(spu_content.name.clone(),spu_content);
    }
    Ok(())
}

async fn handle_update_replica_request(
    req_msg: RequestMessage<UpdateReplicaRequest>,
    ctx: SharedSpuContext,
) -> Result<(), IoError> {
    let (_header, request) = req_msg.get_header_request();
    let req = request.decode_request();
    debug!("spu update replica request: {:#?}", req);
    assert_eq!(req.target_spu, ctx.id());

    Ok(())
}
