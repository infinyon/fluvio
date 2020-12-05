use std::time::{Duration, Instant};
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
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_types::log_on_err;
use flv_util::actions::Actions;

use crate::core::SharedGlobalContext;
use crate::core::SpecChange;
use crate::controllers::follower_replica::ReplicaFollowerController;
use crate::controllers::follower_replica::FollowerReplicaControllerCommand;
use crate::controllers::leader_replica::ReplicaLeaderController;
use crate::controllers::leader_replica::LeaderReplicaState;
use crate::controllers::leader_replica::LeaderReplicaControllerCommand;
use crate::InternalServerError;

use super::SupervisorCommand;
use super::message_sink::{SharedSinkMessageChannel, ScSinkMessageChannel};

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
                    match self.sc_request_loop(socket).await {
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

    /// dispatch sc request
    #[instrument(skip(self, socket))]
    async fn sc_request_loop(&mut self, socket: FlvSocket) -> Result<(), FlvSocketError> {
        use tokio::select;

        /// Interval between each send to SC
        /// SC status are not source of truth, it is delayed derived data.  
        const MIN_SC_SINK_TIME: Duration = Duration::from_millis(200);

        async fn sink_sleep(duration: Duration) {
            if duration < MIN_SC_SINK_TIME {
                sleep(duration).await
            }
        }

        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<InternalSpuRequest, InternalSpuApi>();

        debug!("entering sc request loop");

        let mut sink_time = Instant::now();

        loop {
            if sink_time.elapsed() >= MIN_SC_SINK_TIME {
                if !self.send_status_back_to_sc(&mut sink).await {
                    break;
                }
                sink_time = Instant::now();
            }
            select! {

                _ = sink_sleep(sink_time.elapsed()) =>  {
                    continue;
                },

                sc_request = api_stream.next() => match sc_request {
                    Some(Ok(InternalSpuRequest::UpdateReplicaRequest(request))) => {
                        if let Err(err) = self.handle_update_replica_request(request).await {
                            error!("error handling update replica request: {}", err);
                            break;
                        }
                    },
                    Some(Ok(InternalSpuRequest::UpdateSpuRequest(request))) => {
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

        debug!("exiting sc request loop");

        Ok(())
    }

    async fn send_status_back_to_sc(&mut self, sc_sink: &mut FlvSink) -> bool {
        let requests = self.sink_channel.remove_all().await;
        if !requests.is_empty() {
            let message = RequestMessage::new_request(UpdateLrsRequest::new(requests));

            if let Err(err) = sc_sink.send_request(&message).await {
                error!("error sending batch status to sc: {}", err);
                false
            } else {
                trace!("send status back");
                true
            }
        } else {
            true
        }
    }

    /// register local spu to sc
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
    #[instrument(skip(self, req_msg), name = "update_replica_request")]
    async fn handle_update_replica_request(
        &mut self,
        req_msg: RequestMessage<UpdateReplicaRequest>,
    ) -> Result<(), IoError> {
        let (_, request) = req_msg.get_header_request();

        debug!("received replica update from sc: {:#?}", request);

        let actions = if !request.all.is_empty() {
            debug!(
                epoch = request.epoch,
                item_count = request.all.len(),
                "received replica sync all"
            );
            trace!("received replica all items: {:#?}", request.all);
            self.ctx.replica_localstore().sync_all(request.all)
        } else {
            debug!(
                epoch = request.epoch,
                item_count = request.changes.len(),
                "received replica changes"
            );
            trace!("received replica change items: {:#?}", request.changes);
            self.ctx.replica_localstore().apply_changes(request.changes)
        };

        self.apply_replica_actions(actions).await;
        Ok(())
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

    #[instrument(
        skip(self, actions),
        fields(action_count = actions.count())
    )]
    async fn apply_replica_actions(&self, actions: Actions<SpecChange<Replica>>) {
        if actions.count() == 0 {
            debug!("no replica actions to process. ignoring");
            return;
        }

        trace!("applying replica leader actions");

        let local_id = self.ctx.local_spu_id();

        for replica_action in actions.into_iter() {
            trace!("applying action: {:#?}", replica_action);

            match replica_action {
                SpecChange::Add(new_replica) => {
                    if new_replica.leader == local_id {
                        self.add_leader_replica(new_replica).await;
                    } else {
                        self.add_follower_replica(new_replica).await;
                    }
                }
                SpecChange::Delete(deleted_replica) => {
                    if deleted_replica.leader == local_id {
                        self.remove_leader_replica(deleted_replica).await;
                    } else {
                        self.remove_follower_replica(deleted_replica);
                    }
                }
                SpecChange::Mod(new_replica, old_replica) => {
                    trace!(
                        "replica changed, old: {:#?}, new: {:#?}",
                        new_replica,
                        old_replica
                    );

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
                                self.remove_follower_replica(old_replica);
                                self.add_follower_replica(new_replica).await;
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

    #[instrument(
        skip(self, replica),
        fields(replica_id = &*format!("{}", replica.id))
    )]
    async fn add_leader_replica(&self, replica: Replica) {
        debug!("adding new leader replica");

        let storage_log = self.ctx.config().storage().new_config();
        let replica_id = replica.id.clone();

        match LeaderReplicaState::create_file_replica(replica, &storage_log).await {
            Ok(leader_replica) => {
                debug!("file replica for leader is created: {}", storage_log);
                self.spawn_leader_controller(replica_id, leader_replica)
                    .await;
            }
            Err(err) => {
                error!(
                    "error creating storage leader replica {:#?}, log: {:#?}",
                    err, storage_log
                );
                // TODO: send status back to SC
            }
        }
    }

    #[instrument(
        skip(self, replica),
        fields(replica_id = &*format!("{}", replica.id))
    )]
    async fn update_leader_replica(&self, replica: Replica) {
        debug!("updating leader controller");

        if self.ctx.leaders_state().has_replica(&replica.id) {
            debug!(
                "leader replica was found, sending replica info: {}",
                replica
            );

            match self
                .ctx
                .leaders_state()
                .send_message(
                    &replica.id,
                    LeaderReplicaControllerCommand::UpdateReplicaFromSc(replica.clone()),
                )
                .await
            {
                Ok(status) => {
                    if !status {
                        error!("leader controller mailbox was not found");
                    }
                }
                Err(err) => {
                    error!(
                        "error sending external command to replica controller: {:#?}",
                        err
                    );
                }
            }
        } else {
            error!("leader controller was not found")
        }
    }

    /// reemove leader replica
    #[instrument(
        skip(self, replica),
        fields(replica_id = &*format!("{}", replica.id))
    )]
    async fn remove_leader_replica(&self, replica: Replica) {
        debug!("trying to remove leader controller: {}", replica);

        if self.ctx.leaders_state().has_replica(&replica.id) {
            debug!("leader replica was found, sending removing: {}", replica);

            match self
                .ctx
                .leaders_state()
                .send_message(
                    &replica.id,
                    LeaderReplicaControllerCommand::RemoveReplicaFromSc,
                )
                .await
            {
                Ok(status) => {
                    if !status {
                        error!("leader controller mailbox was not found for: {}", replica);
                    }
                }
                Err(err) => {
                    error!(
                        "error sending external command to replica controller for replica: {}, {:#?}",
                        replica,
                        err
                    );
                }
            }
        } else {
            error!("leader controller: {} was not found", replica)
        }
    }

    /// spawn new leader controller
    #[instrument(
        skip(self, replica_id, leader_state),
        fields(replica_id = &*format!("{}", replica_id))
    )]
    async fn spawn_leader_controller(
        &self,
        replica_id: ReplicaKey,
        leader_state: LeaderReplicaState<FileReplica>,
    ) {
        debug!("spawning new leader controller");

        let (sender, receiver) = bounded(10);

        if let Some(old_replica) = self
            .ctx
            .leaders_state()
            .insert_replica(replica_id.clone(), leader_state, sender)
            .await
        {
            error!(
                "there was existing replica when creating new leader replica: {}",
                old_replica.replica_id()
            );
        }

        let leader_controller = ReplicaLeaderController::new(
            self.ctx.local_spu_id(),
            replica_id,
            receiver,
            self.ctx.leader_state_owned(),
            self.ctx.followers_sink_owned(),
            self.sink_channel.clone(),
            self.ctx.offset_channel().sender(),
            self.max_bytes,
        );
        leader_controller.run();
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
        {
            debug!(
                "old follower replica exists, converting to leader: {}",
                old_replica.id
            );

            let leader_state = LeaderReplicaState::new(
                new_replica.id.clone(),
                new_replica.leader,
                follower_replica.storage_owned(),
                new_replica.replicas,
            );

            self.spawn_leader_controller(new_replica.id, leader_state)
                .await;
        }
    }

    /// Demote leader replica as follower.
    /// This only happens on manual election
    pub async fn demote_replica(&self, replica: Replica) {
        debug!("demoting replica: {}", replica);

        if let Some(leader_replica_state) =
            self.ctx.leaders_state().remove_replica(&replica.id).await
        {
            drop(leader_replica_state);
            // for now, we re-scan file replica
            self.add_follower_replica(replica).await;
        } else {
            error!("leader controller was not found: {}", replica.id)
        }
    }

    /// add new follower controller and it's mailbox
    async fn add_follower_replica(&self, replica: Replica) {
        let leader = &replica.leader;
        debug!("trying to adding follower replica: {}", replica);

        if let Some(sender) = self.ctx.followers_state().mailbox(leader) {
            debug!(
                "existing follower controller exists: {}, send request to controller",
                replica
            );
            log_on_err!(
                sender
                    .send(FollowerReplicaControllerCommand::AddReplica(replica))
                    .await
            )
        } else {
            // we need to spin new follower controller
            debug!(
                "no existing follower controller exists for {},need to spin up",
                replica
            );
            let (sender, receiver) = self.ctx.followers_state().insert_mailbox(*leader);
            let follower_controller = ReplicaFollowerController::new(
                *leader,
                receiver,
                self.ctx.spu_localstore_owned(),
                self.ctx.followers_state_owned(),
                self.ctx.config_owned(),
            );
            follower_controller.run();
            log_on_err!(
                sender
                    .send(FollowerReplicaControllerCommand::AddReplica(replica))
                    .await
            );
        }
    }

    /// update follower replida
    async fn update_follower_replica(&self, replica: Replica) {
        let leader = &replica.leader;
        debug!("trying to adding follower replica: {}", replica);

        if let Some(sender) = self.ctx.followers_state().mailbox(leader) {
            debug!(
                "existing follower controller exists: {}, send update request to controller",
                replica
            );
            log_on_err!(
                sender
                    .send(FollowerReplicaControllerCommand::UpdateReplica(replica))
                    .await
            )
        } else {
            error!("no follower controller found: {}", replica);
        }
    }

    fn remove_follower_replica(&self, replica: Replica) {
        debug!("removing follower replica: {}", replica);
        if self
            .ctx
            .followers_state()
            .remove_replica(&replica.leader, &replica.id)
            .is_none()
        {
            error!("there was no follower replica: {}", replica);
        }
    }
}
