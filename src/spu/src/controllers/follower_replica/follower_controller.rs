use std::time::Duration;

use tracing::{trace, error, debug};
use tracing::instrument;

use tokio::select;
use futures_util::StreamExt;
use async_channel::Receiver;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use fluvio_socket::FlvSocket;
use fluvio_socket::FlvSink;
use fluvio_socket::FlvSocketError;
use dataplane::api::RequestMessage;
use fluvio_controlplane_metadata::partition::Replica;
use fluvio_types::SpuId;
use flv_util::log_on_err;
use fluvio_storage::FileReplica;
use fluvio_controlplane_metadata::spu::SpuSpec;

use crate::controllers::leader_replica::UpdateOffsetRequest;
use crate::services::internal::FetchStreamRequest;
use crate::core::spus::SharedSpuLocalStore;
use crate::core::SharedSpuConfig;

use super::FollowerReplicaControllerCommand;
use super::state::{SharedFollowersBySpu,FollowerReplicaState};
use super::api_key::{FollowerPeerApiEnum};
use super::peer_api::{DefaultSyncRequest,FollowerPeerRequest};

/// time to resync follower offsets to leader
const LEADER_RECONCILIATION_INTERVAL_SEC: u64 = 60; // 1 min

/// Controller for managing follower replicas
/// There is a controller for follower groups (group by leader SPU)
pub struct ReplicaFollowerController<S> {
    leader_id: SpuId,
    spu_localstore: SharedSpuLocalStore,
    followers_state: SharedFollowersBySpu<S>,
    receiver: Receiver<FollowerReplicaControllerCommand>,
    config: SharedSpuConfig,
}

impl<S> ReplicaFollowerController<S> {
    pub fn new(
        leader_id: SpuId,
        receiver: Receiver<FollowerReplicaControllerCommand>,
        spu_localstore: SharedSpuLocalStore,
        followers_state: SharedFollowersBySpu<S>,
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

impl ReplicaFollowerController<FileReplica> {
    pub fn run(self) {
        spawn(self.dispatch_loop());
    }

    pub fn local_spu_id(&self) -> SpuId {
        self.config.id()
    }

    async fn dispatch_loop(mut self) {
        loop {
            if let Some(socket) = self.create_socket_to_leader().await {
                // send initial fetch stream request
                match self.stream_loop(socket).await {
                    Ok(terminate_flag) => {
                        if terminate_flag {
                            debug!("end command has received, terminating connection to leader");
                            break;
                        }
                    }
                    Err(err) => error!("err: {}", err),
                }

                debug!("lost connection to leader, sleeping 5 seconds and will retry it");
                // 5 seconds is heuristic value, may change in the future or could be dynamic
                // depends on back off algorithm
                sleep(Duration::from_secs(5)).await;
            } else {
                debug!("TODO: describe more where this can happen");
                break;
            }
        }
        debug!("shutting down");
    }

    #[instrument(
        name = "FollowerController",
        skip(self),
        fields (
            leader = self.leader_id,
            socket = socket.id()
        )
    )]
    async fn stream_loop(&mut self, mut socket: FlvSocket) -> Result<bool, FlvSocketError> {
        self.send_fetch_stream_request(&mut socket).await?;
        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<FollowerPeerRequest, FollowerPeerApiEnum>();

        // sync offsets
        self.sync_all_offsets_to_leader(&mut sink).await;

        loop {
            debug!("waiting request from leader");

            select! {
                _ = (sleep(Duration::from_secs(LEADER_RECONCILIATION_INTERVAL_SEC))) => {
                    debug!("timer fired - kickoff sync offsets to leader");
                    self.sync_all_offsets_to_leader(&mut sink).await;
                },

                cmd_msg = self.receiver.next() => {
                    if let Some(cmd) = cmd_msg {
                        match cmd {
                            FollowerReplicaControllerCommand::AddReplica(replica) => {
                                debug!(?replica,"received replica replica");
                                self.update_replica(replica).await;
                                self.sync_all_offsets_to_leader(&mut sink).await;
                            },
                            FollowerReplicaControllerCommand::UpdateReplica(replica) => {
                                self.update_replica(replica).await;
                                self.sync_all_offsets_to_leader(&mut sink).await;
                            }
                        }
                    } else {
                        debug!("mailbox to this controller has been closed, shutting controller down");
                        return Ok(true)
                    }
                },
                api_msg = api_stream.next() => {
                    if let Some(req_msg_res) = api_msg {
                        match req_msg_res {
                            Ok(req_msg) => {
                                 match req_msg {
                                    FollowerPeerRequest::SyncRecords(sync_request) => self.write_to_follower_replica(&mut sink,sync_request.request).await,
                                 }

                            },
                            Err(err) => {
                                error!("error decoding request: {}, terminating connection",err);
                                return Ok(false)
                            }
                        }
                    } else {
                        debug!("leader socket has terminated");
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

            debug!("leader spu spec is not available, waiting 1 second");
            sleep(Duration::from_millis(1000)).await;
            debug!("awake from sleep, checking spus");
        }
    }

    async fn write_to_follower_replica(&self, sink: &mut FlvSink, req: DefaultSyncRequest) {
        debug!("received sync req");
        let offsets = self.followers_state.write_topics(req).await;
        self.sync_offsets_to_leader(sink, offsets).await;
    }

    /// connect to leader, if can't connect try until we succeed
    /// or if we received termination message
    async fn create_socket_to_leader(&mut self) -> Option<FlvSocket> {
        let leader_spu = self.get_spu().await;
        let leader_endpoint = leader_spu.private_endpoint.to_string();
        loop {
            debug!(
                %leader_endpoint,
                "trying to create socket to leader",
            );
            let connect_future = FlvSocket::connect(&leader_endpoint);

            select! {
                msg = self.receiver.next() => {

                    debug!("connected to leader");

                    // if connection is successful, need to synchronize replica metadata with leader
                    if let Some(cmd) = msg {
                        match cmd {
                            FollowerReplicaControllerCommand::AddReplica(replica) => self.update_replica(replica).await,
                            FollowerReplicaControllerCommand::UpdateReplica(replica) => self.update_replica(replica).await
                        }
                    } else {
                        error!("mailbox seems terminated, controller is going to be terminated");
                        return None
                    }
                },
                socket_res = connect_future => {
                    match socket_res {
                        Ok(socket) => {
                            trace!("connected to leader");
                            return Some(socket)
                        }
                        Err(err) => error!("error connecting err: {}",err)
                    }

                    debug!("sleeping 1 seconds to connect to leader");
                    sleep(Duration::from_secs(1)).await;
                }

            }
        }
    }

    /// send request to establish peer to peer communication to leader
    async fn send_fetch_stream_request(
        &self,
        socket: &mut FlvSocket,
    ) -> Result<(), FlvSocketError> {
        let local_spu_id = self.local_spu_id();
        debug!("sending fetch stream for leader",);
        let fetch_request = FetchStreamRequest {
            spu_id: local_spu_id,
            ..Default::default()
        };
        let mut message = RequestMessage::new_request(fetch_request);
        message
            .get_mut_header()
            .set_client_id(format!("peer spu: {}", local_spu_id));

        let response = socket.send(&message).await?;
        trace!(?response, "follower: fetch stream response",);
        debug!("follower: established peer to peer channel to leader",);
        Ok(())
    }

    /// create new replica if doesn't exist yet
    async fn update_replica(&self, replica_msg: Replica) {
        debug!(?replica_msg, "received update replica",);

        let replica_key = replica_msg.id.clone();
        if self.followers_state.has_replica(&replica_key) {
            debug!(
                %replica_key,
                "has already follower replica, ignoring",
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
    async fn sync_all_offsets_to_leader(&self, sink: &mut FlvSink) {
        self.sync_offsets_to_leader(sink, self.followers_state.replica_offsets(&self.leader_id))
            .await;
    }

    /// send follower offset to leader
    async fn sync_offsets_to_leader(&self, sink: &mut FlvSink, offsets: UpdateOffsetRequest) {
        debug!(?offsets, "sending offsets to leader");
        let req_msg = RequestMessage::new_request(offsets)
            .set_client_id(format!("follower_id: {}", self.config.id()));

        trace!(?req_msg, "sending offsets leader",);

        log_on_err!(
            sink.send_request(&req_msg).await,
            "error sending request to leader {}"
        );
    }
}
