use std::{time::Duration};
use std::io::Error as IoError;
use std::sync::Arc;

use tracing::{info, trace, error, debug, warn, instrument};
use flv_util::print_cli_err;

use tokio::select;
use futures_util::stream::StreamExt;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use fluvio_controlplane::InternalSpuApi;
use fluvio_controlplane::InternalSpuRequest;
use fluvio_controlplane::RegisterSpuRequest;
use fluvio_controlplane::{UpdateSpuRequest, UpdateLrsRequest};
use fluvio_controlplane::UpdateReplicaRequest;
use dataplane::api::RequestMessage;
use fluvio_socket::{FluvioSocket, SocketError, FluvioSink};
use crate::core::GlobalContext;
use crate::InternalServerError;
use crate::control_plane::StatusMessageSink;

// keep track of various internal state of dispatcher
#[derive(Default)]
struct DispatcherCounter {
    pub replica_changes: u64, // replica changes received from sc
    pub spu_changes: u64,     // spu changes received from sc
    pub reconnect: u64,       // number of reconnect to sc
}

/// Controller for handling connection to SC
/// including registering and reconnect
pub struct ScDispatcher {
    ctx: Arc<GlobalContext>,
    status_update: Arc<StatusMessageSink>,
    counter: DispatcherCounter,
}

impl ScDispatcher {
    pub fn new(ctx: Arc<GlobalContext>) -> Self {
        Self {
            status_update: ctx.status_update_owned(),
            ctx,
            counter: DispatcherCounter::default(),
        }
    }

    /// start the controller with ctx and receiver
    pub fn run(self) {
        spawn(self.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        let mut counter: u64 = 0;

        const WAIT_RECONNECT_INTERVAL: u64 = 3000;

        loop {
            debug!("entering SC dispatch loop: {}", counter);

            if let Some(mut socket) = self.create_socket_to_sc().await {
                debug!(
                    "established connection to sc for spu: {}",
                    self.ctx.local_spu_id()
                );

                // register and exit on error
                let status = match self.send_spu_registeration(&mut socket).await {
                    Ok(status) => status,
                    Err(err) => {
                        print_cli_err!(format!(
                            "spu registeration failed with sc due to error: {}",
                            err
                        ));
                        false
                    }
                };

                if !status {
                    warn!("sleeping 3 seconds before re-trying re-register");
                    sleep(Duration::from_millis(WAIT_RECONNECT_INTERVAL)).await;
                } else {
                    // continuously process updates from and send back status to SC
                    match self.request_loop(socket).await {
                        Ok(_) => {
                            debug!(
                                "sc connection terminated: {}, waiting before reconnecting",
                                counter
                            );
                            // give little bit time before trying to reconnect
                            sleep(Duration::from_millis(10)).await;
                            counter += 1;
                        }
                        Err(err) => {
                            warn!(
                                "error connecting to sc: {:#?}, waiting before reconnecting",
                                err
                            );
                            // We are  connection to sc.  Retry again
                            // Currently we use 3 seconds to retry but this should be using backoff algorithm
                            sleep(Duration::from_millis(WAIT_RECONNECT_INTERVAL)).await;
                        }
                    }
                }
            }
        }
    }

    #[instrument(
        skip(self),
        name = "sc_dispatch_loop",
        fields(
            socket = socket.id()
        )
    )]
    async fn request_loop(&mut self, socket: FluvioSocket) -> Result<(), SocketError> {
        use async_io::Timer;

        /// Interval between each send to SC
        /// SC status are not source of truth, it is delayed derived data.  
        const MIN_SC_SINK_TIME: Duration = Duration::from_millis(400);

        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<InternalSpuRequest, InternalSpuApi>();

        let mut status_timer = Timer::interval(MIN_SC_SINK_TIME);

        loop {
            trace!("waiting");

            select! {

                _ = status_timer.next() =>  {
                    trace!("status timer expired");
                    if !self.send_status_back_to_sc(&mut sink).await {
                        debug!("error sending status, exiting request loop");
                        break;
                    }
                },

                sc_request = api_stream.next() => {
                    debug!("got requests from sc");
                    match sc_request {
                        Some(Ok(InternalSpuRequest::UpdateReplicaRequest(request))) => {
                            self.counter.replica_changes += 1;
                            self.handle_update_replica_request(request,&mut sink).await;
                        },
                        Some(Ok(InternalSpuRequest::UpdateSpuRequest(request))) => {
                            self.counter.spu_changes += 1;
                            if let Err(err) = self.handle_update_spu_request(request).await {
                                error!("error handling update spu request: {}", err);
                                break;
                            }
                        },
                        Some(_) => {
                            debug!("no more sc msg content, end");
                            break;
                        },
                        _ => {
                            debug!("sc connection terminated");
                            break;
                        }
                    }
                }

            }
        }

        debug!("exiting sc request loop");

        Ok(())
    }

    /// send status back to sc, if there is error return false
    #[instrument(skip(self))]
    async fn send_status_back_to_sc(&mut self, sc_sink: &mut FluvioSink) -> bool {
        let requests = self.status_update.remove_all().await;
        if !requests.is_empty() {
            debug!(requests = ?requests, "sending status back to sc");
            let message = RequestMessage::new_request(UpdateLrsRequest::new(requests));

            if let Err(err) = sc_sink.send_request(&message).await {
                error!("error sending batch status to sc: {}", err);
                false
            } else {
                debug!("successfully send status back to sc");
                true
            }
        } else {
            trace!("nothing to send back to sc");
            true
        }
    }

    /// register local spu to sc
    #[instrument(
        skip(self),
        fields(socket = socket.id())
    )]
    async fn send_spu_registeration(
        &self,
        socket: &mut FluvioSocket,
    ) -> Result<bool, InternalServerError> {
        let local_spu_id = self.ctx.local_spu_id();

        debug!("sending spu '{}' registration request", local_spu_id);

        let register_req = RegisterSpuRequest::new(local_spu_id);
        let mut message = RequestMessage::new_request(register_req);
        message
            .get_mut_header()
            .set_client_id(format!("spu: {}", local_spu_id));

        let response = socket.send(&message).await?;

        trace!("register response: {:#?}", response);

        let register_resp = &response.response;
        if register_resp.is_error() {
            warn!(
                "spu '{}' registration failed: {}",
                local_spu_id,
                register_resp.error_message()
            );

            Ok(false)
        } else {
            info!("spu '{}' registration successful", local_spu_id);

            Ok(true)
        }
    }

    /// connect to sc if can't connect try until we succeed
    /// or if we received termination message
    async fn create_socket_to_sc(&mut self) -> Option<FluvioSocket> {
        let spu_id = self.ctx.local_spu_id();
        let sc_endpoint = self.ctx.config().sc_endpoint().to_string();

        debug!("trying to connect to sc endpoint: {}", sc_endpoint);

        let wait_interval = self.ctx.config().sc_retry_ms;
        loop {
            trace!(
                "trying to create socket to sc: {:#?} for spu: {}",
                sc_endpoint,
                spu_id
            );
            let connect_future = FluvioSocket::connect(&sc_endpoint);

            select! {
                socket_res = connect_future => {
                    match socket_res {
                        Ok(socket) => {
                            debug!("connected to sc for spu: {}",spu_id);
                            self.counter.reconnect += 1;
                            return Some(socket)
                        }
                        Err(err) => warn!("error connecting to sc: {}",err)
                    }

                    trace!("sleeping {} ms to connect to sc: {}",wait_interval,spu_id);
                    sleep(Duration::from_millis(wait_interval as u64)).await;
                }
            }
        }
    }

    ///
    /// Follower Update Handler sent by a peer Spu
    ///
    #[instrument(skip(self, req_msg))]
    async fn handle_update_replica_request(
        &mut self,
        req_msg: RequestMessage<UpdateReplicaRequest>,
        sc_sink: &mut FluvioSink,
    ) {
        use crate::core::ReplicaChange;

        let (_, request) = req_msg.get_header_request();

        debug!( message = ?request,"replica request");

        for action in self.ctx.apply_replica_update(request).await.into_iter() {
            match action {
                ReplicaChange::Remove(remove) => {
                    let message = RequestMessage::new_request(remove);
                    if let Err(err) = sc_sink.send_request(&message).await {
                        error!("error sending back to sc {}", err);
                    }
                }
                ReplicaChange::StorageError(err) => {
                    error!("error storage {}", err);
                }
            }
        }
    }

    ///
    /// Follower Update Handler sent by a peer Spu
    ///
    #[instrument(skip(self, req_msg), name = "update_spu_request")]
    async fn handle_update_spu_request(
        &mut self,
        req_msg: RequestMessage<UpdateSpuRequest>,
    ) -> Result<(), IoError> {
        let (_, request) = req_msg.get_header_request();

        debug!( message = ?request,"starting spu update");

        let _actions = if !request.all.is_empty() {
            debug!(
                epoch = request.epoch,
                item_count = request.all.len(),
                "received spu sync all"
            );
            trace!("received spu all items: {:#?}", request.all);
            self.ctx.spu_localstore().sync_all(request.all)
        } else {
            debug!(
                epoch = request.epoch,
                item_count = request.changes.len(),
                "received spu changes"
            );
            trace!("received spu change items: {:#?}", request.changes);
            self.ctx.spu_localstore().apply_changes(request.changes)
        };

        self.ctx.sync_follower_update().await;

        trace!("finish spu update");

        Ok(())
    }
}
