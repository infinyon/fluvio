use std::time::Duration;

use log::trace;
use log::error;
use log::debug;

use futures::channel::mpsc::Receiver;
use futures::select;
use futures::StreamExt;
use futures::FutureExt;

use flv_future_aio::task::spawn;
use flv_future_aio::timer::sleep;
use kf_socket::KfSocket;
use kf_socket::KfSink;
use kf_socket::KfSocketError;
use kf_protocol::api::RequestMessage;
use flv_metadata::partition::Replica;
use flv_types::SpuId;
use flv_util::log_on_err;
use flv_storage::FileReplica;
use flv_metadata::spu::SpuSpec;

use crate::controllers::leader_replica::UpdateOffsetRequest;
use crate::services::internal::FetchStreamRequest;
use crate::core::spus::SharedSpuLocalStore;
use crate::core::SharedSpuConfig;

use super::FollowerReplicaControllerCommand;
use super::FollowerReplicaState;
use super::KfFollowerPeerApiEnum;
use super::DefaultSyncRequest;
use super::FollowerPeerRequest;
use super::SharedFollowersState;

/// time to resync follower offsets to leader
const LEADER_RECONCILIATION_INTERVAL_SEC: u64 = 60; // 1 min

/// Controller for managing follower replicas
/// There is a controller for follower groups (group by leader SPU)
pub struct ReplicaFollowerController<S> {
    leader_id: SpuId,
    spu_localstore: SharedSpuLocalStore,
    followers_state: SharedFollowersState<S>,
    receiver: Receiver<FollowerReplicaControllerCommand>,
    config: SharedSpuConfig,
}

impl<S> ReplicaFollowerController<S> {
    pub fn new(
        leader_id: SpuId,
        receiver: Receiver<FollowerReplicaControllerCommand>,
        spu_localstore: SharedSpuLocalStore,
        followers_state: SharedFollowersState<S>,
        config: SharedSpuConfig,
    ) -> Self {
        Self {
            leader_id,
            spu_localstore,
            receiver,
            followers_state,
            config,
        }
    }
}

/// debug leader, this should be only used in replica leader controller
/// example:  leader_debug!("{}",expression)
macro_rules! follower_log {
    ($log:ident, $self:ident, $message:expr,$($arg:expr)*) => (
        $log!(
            concat!("follower with leader: {}",$message),
            $self.leader_id,
            $($arg)*
        )
    );

    ($log:ident,$self:ident, $message:expr) => (
        $log!(
            concat!("follower: with leader: {}",$message),
            $self.leader_id
        )
    )
}

macro_rules! follower_debug {
    ($self:ident, $message:expr,$($arg:expr)*) => ( follower_log!(debug,$self,$message,$($arg)*));

    ($self:ident, $message:expr) => ( follower_log!(debug,$self, $message))
}

macro_rules! follower_error {
    ($self:ident, $message:expr,$($arg:expr)*) => ( follower_log!(error,$self,$message,$($arg)*));

    ($self:ident, $message:expr) => ( follower_log!(error,$self, $message))
}

impl ReplicaFollowerController<FileReplica> {
    pub fn run(self) {
        spawn(self.dispatch_loop());
    }

    pub fn local_spu_id(&self) -> SpuId {
        self.config.id()
    }

    async fn dispatch_loop(mut self) {
        follower_debug!(self, "starting");
        loop {
            if let Some(socket) = self.create_socket_to_leader().await {
                // send initial fetch stream request
                match self.stream_loop(socket).await {
                    Ok(terminate_flag) => {
                        if terminate_flag {
                            follower_debug!(
                                self,
                                "end command has received, terminating connection to leader"
                            );
                            break;
                        }
                    }
                    Err(err) => follower_error!(self, "err: {}", err),
                }

                follower_debug!(
                    self,
                    "lost connection to leader, sleeping 5 seconds and will retry it"
                );
                // 5 seconds is heuristic value, may change in the future or could be dynamic
                // depends on back off algorithm
                sleep(Duration::from_secs(5)).await;
            } else {
                follower_debug!(self, "TODO: describe more where this can happen");
                break;
            }
        }
        follower_debug!(self, "shutting down");
    }

    async fn stream_loop(&mut self, mut socket: KfSocket) -> Result<bool, KfSocketError> {
        self.send_fetch_stream_request(&mut socket).await?;
        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<FollowerPeerRequest, KfFollowerPeerApiEnum>();

        // sync offsets
        self.sync_all_offsets_to_leader(&mut sink).await;

        loop {
            follower_debug!(self, "waiting request from leader");

            select! {
                _ = (sleep(Duration::from_secs(LEADER_RECONCILIATION_INTERVAL_SEC))).fuse() => {
                    follower_debug!(self,"timer fired - kickoff sync offsets to leader");
                    self.sync_all_offsets_to_leader(&mut sink).await;
                },

                cmd_msg = self.receiver.next() => {
                    if let Some(cmd) = cmd_msg {
                        match cmd {
                            FollowerReplicaControllerCommand::AddReplica(replica) => {
                                follower_debug!(self,"received replica replica: {}",replica);
                                self.update_replica(replica).await;
                                self.sync_all_offsets_to_leader(&mut sink).await;
                            },
                            FollowerReplicaControllerCommand::UpdateReplica(replica) => {
                                self.update_replica(replica).await;
                                self.sync_all_offsets_to_leader(&mut sink).await;
                            }
                        }
                    } else {
                        follower_debug!(self,"mailbox to this controller has been closed, shutting controller down");
                        return Ok(true)
                    }
                },
                api_msg = api_stream.next().fuse() => {
                    if let Some(req_msg_res) = api_msg {
                        match req_msg_res {
                            Ok(req_msg) => {
                                 match req_msg {
                                    FollowerPeerRequest::SyncRecords(sync_request) => self.write_to_follower_replica(&mut sink,sync_request.request).await,
                                 }

                            },
                            Err(err) => {
                                follower_error!(self, "error decoding request: {}, terminating connection",err);
                                 return Ok(false)
                            }
                        }
                    } else {
                        follower_debug!(self,"leader socket has terminated");
                        return Ok(false);

                    }
                }
            }
        }
    }

    /// get available spu, this is case where follower request is received before rest of spu arrives from SC.
    /// TODO: remove wait call
    async fn get_spu(&self) -> SpuSpec {
        loop {
            if let Some(spu) = self.spu_localstore.spec(&self.leader_id) {
                return spu;
            }

            follower_debug!(self, "leader spu spec is not available, waiting 1 second");
            sleep(Duration::from_millis(1000)).await;
            follower_debug!(self, "awake from sleep, checking spus");
        }
    }

    async fn write_to_follower_replica(&self, sink: &mut KfSink, req: DefaultSyncRequest) {
        follower_debug!(self, "handling sync request from req {}", req);

        let offsets = self.followers_state.send_records(req).await;
        self.sync_offsets_to_leader(sink, offsets).await;
    }

    /// connect to leader, if can't connect try until we succeed
    /// or if we received termination message
    async fn create_socket_to_leader(&mut self) -> Option<KfSocket> {
        let leader_spu = self.get_spu().await;
        let leader_endpoint = leader_spu.private_endpoint.to_string();
        loop {
            follower_debug!(
                self,
                "trying to create socket to leader at: {}",
                leader_endpoint
            );
            let connect_future = KfSocket::connect(&leader_endpoint);

            select! {
                msg = self.receiver.next() => {

                    follower_debug!(self,"connected to leader");

                    // if connection is successful, need to synchronize replica metadata with leader
                    if let Some(cmd) = msg {
                        match cmd {
                            FollowerReplicaControllerCommand::AddReplica(replica) => self.update_replica(replica).await,
                            FollowerReplicaControllerCommand::UpdateReplica(replica) => self.update_replica(replica).await
                        }
                    } else {
                        follower_error!(self,"mailbox seems terminated, controller is going to be terminated");
                        return None
                    }
                },
                socket_res = connect_future.fuse() => {
                    match socket_res {
                        Ok(socket) => {
                            trace!("connected to leader: {}",self.leader_id);
                            return Some(socket)
                        }
                        Err(err) => follower_error!(self,"error connecting err: {}",err)
                    }

                    follower_debug!(self,"sleeping 1 seconds to connect to leader");
                    sleep(Duration::from_secs(1)).await;
                }

            }
        }
    }

    /// send request to establish peer to peer communication to leader
    async fn send_fetch_stream_request(&self, socket: &mut KfSocket) -> Result<(), KfSocketError> {
        let local_spu_id = self.local_spu_id();
        trace!(
            "follower: {}, sending fetch stream for leader: {}",
            local_spu_id,
            self.leader_id,
        );
        let mut fetch_request = FetchStreamRequest::default();
        fetch_request.spu_id = local_spu_id;
        let mut message = RequestMessage::new_request(fetch_request);
        message
            .get_mut_header()
            .set_client_id(format!("peer spu: {}", local_spu_id));

        let response = socket.send(&message).await?;
        trace!(
            "follower: {}, fetch stream response: {:#?}",
            local_spu_id,
            response
        );
        debug!(
            "follower: {}, established peer to peer channel to leader: {}",
            local_spu_id, self.leader_id,
        );
        Ok(())
    }

    /// create new replica if doesn't exist yet
    async fn update_replica(&self, replica_msg: Replica) {
        debug!(
            "follower: {}, received update replica {} from leader: {}",
            self.local_spu_id(),
            replica_msg,
            self.leader_id
        );

        let replica_key = replica_msg.id.clone();
        if self.followers_state.has_replica(&replica_key) {
            debug!(
                "follower: {}, has already follower replica: {}, ignoring",
                self.local_spu_id(),
                replica_key
            );
        } else {
            let log = &self.config.storage().new_config();
            match FollowerReplicaState::new(
                self.config.id(),
                replica_msg.leader,
                &replica_key,
                &log,
            )
            .await
            {
                Ok(replica_state) => {
                    self.followers_state.insert_replica(replica_state);
                }
                Err(err) => error!(
                    "follower: {}, error creating follower replica: {}, error: {:#?}",
                    self.local_spu_id(),
                    replica_key,
                    err
                ),
            }
        }
    }

    /// send offset to leader, so it can chronize
    async fn sync_all_offsets_to_leader(&self, sink: &mut KfSink) {
        self.sync_offsets_to_leader(sink, self.followers_state.replica_offsets(&self.leader_id))
            .await;
    }

    /// send follower offset to leader
    async fn sync_offsets_to_leader(&self, sink: &mut KfSink, offsets: UpdateOffsetRequest) {
        let req_msg = RequestMessage::new_request(offsets)
            .set_client_id(format!("follower_id: {}", self.config.id()));

        trace!(
            "follower: {}, sending offsets: {:#?} to leader: {}",
            self.local_spu_id(),
            &req_msg,
            self.leader_id
        );

        log_on_err!(
            sink.send_request(&req_msg).await,
            "error sending request to leader {}"
        );
        debug!(
            "follower: {}, synced follower offset: {} to leader: {}",
            self.local_spu_id(),
            self.config.id(),
            self.leader_id
        );
    }
}
