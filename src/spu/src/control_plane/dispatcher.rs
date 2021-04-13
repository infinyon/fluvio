use std::{time::Duration};
use std::io::Error as IoError;

use tracing::info;
use tracing::trace;
use tracing::error;
use tracing::debug;
use tracing::warn;
use tracing::instrument;
use flv_util::print_cli_err;

use async_channel::Receiver;
use async_channel::Sender;
use async_channel::bounded;
use tokio::select;
use futures_util::stream::StreamExt;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use fluvio_controlplane::InternalSpuApi;
use fluvio_controlplane::InternalSpuRequest;
use fluvio_controlplane::RegisterSpuRequest;
use fluvio_controlplane::{UpdateSpuRequest, UpdateLrsRequest};
use fluvio_controlplane::UpdateReplicaRequest;
use fluvio_controlplane_metadata::partition::Replica;
use dataplane::api::RequestMessage;
use fluvio_socket::{FlvSocket, FlvSocketError, FlvSink};
use fluvio_storage::FileReplica;
use flv_util::actions::Actions;

use crate::core::SharedGlobalContext;
use crate::core::SpecChange;
use crate::replication::leader::{LeaderReplicaState, LeaderReplicaControllerCommand};
use crate::InternalServerError;

use super::SupervisorCommand;
use super::message_sink::{SharedSinkMessageChannel, ScSinkMessageChannel};

// keep track of various internal state of dispatcher
#[derive(Default)]
struct DispatcherCounter {
    pub replica_changes: u64, // replica changes received from sc
    pub spu_changes: u64,     // spu changes received from sc
    pub status_send: u64,     // number of status send to sc
    pub reconnect: u64,       // number of reconnect to sc
}

/// Controller for handling connection to SC
/// including registering and reconnect
pub struct ScDispatcher<S> {
    termination_receiver: Receiver<bool>,
    #[allow(dead_code)]
    termination_sender: Sender<bool>,
    #[allow(dead_code)]
    supervisor_command_sender: Sender<SupervisorCommand>,
    ctx: SharedGlobalContext<S>,
    max_bytes: u32,
    sink_channel: SharedSinkMessageChannel,
    counter: DispatcherCounter,
}

impl<S> ScDispatcher<S> {
    pub fn new(ctx: SharedGlobalContext<S>, max_bytes: u32) -> Self {
        let (termination_sender, termination_receiver) = bounded(1);
        let (supervisor_command_sender, _supervisor_command_receiver) = bounded(100);
        let sink_channel = ScSinkMessageChannel::shared();
        Self {
            termination_receiver,
            termination_sender,
            supervisor_command_sender,
            ctx,
            max_bytes,
            sink_channel,
            counter: DispatcherCounter::default(),
        }
    }
}

impl ScDispatcher<FileReplica> {
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
    async fn request_loop(&mut self, socket: FlvSocket) -> Result<(), FlvSocketError> {
        use async_io::Timer;

        /// Interval between each send to SC
        /// SC status are not source of truth, it is delayed derived data.  
        const MIN_SC_SINK_TIME: Duration = Duration::from_millis(400);

        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<InternalSpuRequest, InternalSpuApi>();

        let mut status_timer = Timer::interval(MIN_SC_SINK_TIME);

        loop {
            select! {

                _ = status_timer.next() =>  {
                    trace!("status timer expired");
                    if !self.send_status_back_to_sc(&mut sink).await {
                        debug!("error sending status, exiting request loop");
                        break;
                    }
                },

                sc_request = api_stream.next() => {
                    trace!("got requests from sc");
                    match sc_request {
                        Some(Ok(InternalSpuRequest::UpdateReplicaRequest(request))) => {
                            self.counter.replica_changes += 1;
                            if let Err(err) = self.handle_update_replica_request(request,&mut sink).await {
                                error!("error handling update replica request: {}", err);
                                break;
                            }
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
    async fn send_status_back_to_sc(&mut self, sc_sink: &mut FlvSink) -> bool {
        let requests = self.sink_channel.remove_all().await;
        if !requests.is_empty() {
            trace!(requests = ?requests, "sending status back to sc");
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
        socket: &mut FlvSocket,
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
    async fn create_socket_to_sc(&mut self) -> Option<FlvSocket> {
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
            let connect_future = FlvSocket::connect(&sc_endpoint);

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
                },
                _ = self.termination_receiver.next() => {
                    info!("termination message received");
                    return None
                }
            }
        }
    }

    ///
    /// Follower Update Handler sent by a peer Spu
    ///
    #[instrument(
        skip(self, sc_sink, req_msg),
        name = "update_replica_request",
        fields(replica_changes = self.counter.replica_changes))
    ]
    async fn handle_update_replica_request(
        &mut self,
        req_msg: RequestMessage<UpdateReplicaRequest>,
        sc_sink: &mut FlvSink,
    ) -> Result<(), FlvSocketError> {
        let (_, request) = req_msg.get_header_request();

        debug!( message = ?request,"replica request");

        let actions = if !request.all.is_empty() {
            trace!("received replica all items: {:#?}", request.all);
            self.ctx.replica_localstore().sync_all(request.all)
        } else {
            trace!("received replica change items: {:#?}", request.changes);
            self.ctx.replica_localstore().apply_changes(request.changes)
        };

        self.apply_replica_actions(actions, sc_sink).await
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

        debug!( message = ?request,"spu request");

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

        Ok(())
    }

    #[instrument(skip(self, actions, sc_sink))]
    async fn apply_replica_actions(
        &self,
        actions: Actions<SpecChange<Replica>>,
        sc_sink: &mut FlvSink,
    ) -> Result<(), FlvSocketError> {
        trace!( actions = ?actions,"replica actions");

        if actions.count() == 0 {
            debug!("no replica actions to process. ignoring");
            return Ok(());
        }

        trace!("applying replica leader actions");

        let local_id = self.ctx.local_spu_id();

        for replica_action in actions.into_iter() {
            trace!("applying action: {:#?}", replica_action);

            match replica_action {
                SpecChange::Add(new_replica) => {
                    if new_replica.leader == local_id {
                        if new_replica.is_being_deleted {
                            self.remove_leader_replica(new_replica, sc_sink).await?;
                        } else if let Err(err) = self
                            .ctx
                            .leaders_state()
                            .add_leader_replica(
                                self.ctx.clone(),
                                new_replica,
                                self.max_bytes,
                                self.sink_channel.clone(),
                            )
                            .await
                        {
                            error!("error creating leader replica: {}", err);
                        }
                    } else if new_replica.is_being_deleted {
                        self.remove_follower_replica(new_replica).await;
                    } else {
                        if let Err(err) = self
                            .ctx
                            .followers_state_owned()
                            .add_replica(self.ctx.clone(), new_replica)
                            .await
                        {
                            error!("adding replica failed: {}", err);
                        }
                    }
                }
                SpecChange::Delete(deleted_replica) => {
                    if deleted_replica.leader == local_id {
                        self.remove_leader_replica(deleted_replica, sc_sink).await?;
                    } else {
                        self.remove_follower_replica(deleted_replica).await;
                    }
                }
                SpecChange::Mod(new_replica, old_replica) => {
                    trace!(
                        "replica changed, old: {:#?}, new: {:#?}",
                        new_replica,
                        old_replica
                    );

                    if new_replica.is_being_deleted {
                        if new_replica.leader == local_id {
                            self.remove_leader_replica(new_replica, sc_sink).await?;
                        } else {
                            self.remove_follower_replica(new_replica).await;
                        }
                    } else {
                        // check for leader change
                        if new_replica.leader != old_replica.leader {
                            if new_replica.leader == local_id {
                                // we become leader
                                self.promote_replica(new_replica, old_replica).await;
                            } else {
                                // we are follower
                                // if we were leader before, we demote out self
                                if old_replica.leader == local_id {
                                    self.demote_replica(new_replica).await;
                                } else {
                                    // we stay as follower but we switch to new leader
                                    debug!("still follower but switching leader: {}", new_replica);
                                    self.switch_leader_for_follower(new_replica, old_replica)
                                        .await;
                                }
                            }
                        } else if new_replica.leader == local_id {
                            self.update_leader_replica(new_replica).await;
                        } else {
                            self.update_follower_replica(new_replica).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    #[instrument(skip(self, replica),
        fields(replica = %replica.id)
    )]
    async fn update_leader_replica(&self, replica: Replica) {
        debug!("updating leader controller");

        if let Some(leader_state) = self.ctx.leaders_state().get(&replica.id) {
            debug!(
                "leader replica was found, sending replica info: {}",
                replica
            );

            if let Err(err) = leader_state
                .send_message_to_controller(LeaderReplicaControllerCommand::UpdateReplicaFromSc(
                    replica.clone(),
                ))
                .await
            {
                error!(
                    "error sending external command to replica controller: {:#?}",
                    err
                );
            }
        } else {
            error!("leader controller was not found: {}", replica.id);
        }
    }

    /// reemove leader replica
    #[instrument(
        skip(self,replica,sc_sink),
        fields(
            replica = %replica.id,
        )
    )]
    async fn remove_leader_replica(
        &self,
        replica: Replica,
        sc_sink: &mut FlvSink,
    ) -> Result<(), FlvSocketError> {
        use fluvio_controlplane::ReplicaRemovedRequest;

        // try to send message to leader controller if still exists
        debug!("sending terminate message to leader controller");
        let confirm =
            if let Some((_, previous_state)) = self.ctx.leaders_state().remove(&replica.id) {
                if let Err(err) = previous_state
                    .send_message_to_controller(LeaderReplicaControllerCommand::RemoveReplicaFromSc)
                    .await
                {
                    error!(
                    "error sending external command to replica controller for replica: {}, {:#?}",
                    replica, err
                );
                }

                if let Err(err) = previous_state.remove().await {
                    error!("error: {} removing replica: {}", err, replica);
                } else {
                    debug!(
                        replica = %replica.id,
                        "leader remove was removed"
                    );
                }
                true
            } else {
                // if we don't find existing replica, just warning
                warn!("no existing replica found {}", replica);

                //LeaderReplicaState::clear_file_replica(&replica, &self.ctx.config().log).await;

                true
            };

        let confirm_request = ReplicaRemovedRequest::new(replica.id, confirm);
        debug!(
            sc_message = ?confirm_request,
            "sending back delete confirmation to sc"
        );

        let message = RequestMessage::new_request(confirm_request);

        sc_sink.send_request(&message).await
    }

    /// Promote follower replica as leader,
    /// This is done in 3 steps
    /// // 1: Remove follower replica from followers state
    /// // 2: Terminate followers controller if need to be (if there are no more follower replicas for that controller)
    /// // 3: Start leader controller
    pub async fn promote_replica(&self, new_replica: Replica, old_replica: Replica) {
        debug!("promoting replica: {} from: {}", new_replica, old_replica);

        if let Some(follower_replica) = self
            .ctx
            .followers_state()
            .remove_replica(&old_replica.leader, &old_replica.id)
            .await
        {
            debug!(
                "old follower replica exists, converting to leader: {}",
                old_replica.id
            );

            let (sender, receiver) = bounded(10);

            let leader_state = LeaderReplicaState::promoted_from(
                follower_replica,
                new_replica.clone(),
                self.ctx.config().into(),
                sender,
            );

            self.ctx
                .leaders_state()
                .spawn_leader_controller(
                    self.ctx.clone(),
                    new_replica.id,
                    leader_state,
                    receiver,
                    self.max_bytes,
                    self.sink_channel.clone(),
                )
                .await;
        }
    }

    /// Demote leader replica as follower.
    /// This only happens on manual election
    pub async fn demote_replica(&self, replica: Replica) {
        debug!("demoting replica: {}", replica);

        if let Some(leader_replica_state) = self.ctx.leaders_state().remove(&replica.id) {
            drop(leader_replica_state);
            if let Err(err) = self
                .ctx
                .followers_state_owned()
                .add_replica(self.ctx.clone(), replica)
                .await
            {
                error!("demotion failed: {}", err);
            }
        } else {
            error!("leader controller was not found: {}", replica.id)
        }
    }

    /// update follower replida
    async fn update_follower_replica(&self, replica: Replica) {
        debug!("trying to adding follower replica: {}", &replica.leader);

        self.ctx.followers_state().update_replica(replica).await;
    }

    async fn remove_follower_replica(&self, replica: Replica) {
        debug!("removing follower replica: {}", replica);
        if let Some(replica_state) = self
            .ctx
            .followers_state()
            .remove_replica(&replica.leader, &replica.id)
            .await
        {
            if let Err(err) = replica_state.remove().await {
                error!("error {}, removing replica: {}", err, replica);
            }
        } else {
            error!("there was no follower replica: {} to remove", replica);
        }
    }

    async fn switch_leader_for_follower(&self, new: Replica, old: Replica) {
        // we stay as follower but we switch to new leader
        debug!("still follower but switching leader: {}", new);
        if self
            .ctx
            .followers_state()
            .remove_replica(&old.leader, &old.id)
            .await
            .is_none()
        {
            error!("there was no follower replica: {} to switch", new);
        }
        if let Err(err) = self
            .ctx
            .followers_state_owned()
            .add_replica(self.ctx.clone(), new)
            .await
        {
            error!("leader switch failed: {}", err);
        }
    }
}
