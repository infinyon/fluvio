use std::{collections::HashMap, sync::Arc, time::Duration};

use tracing::{trace, error, debug};
use tracing::instrument;

use tokio::select;
use futures_util::StreamExt;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use fluvio_socket::FlvSocket;
use fluvio_socket::FlvSink;
use fluvio_socket::FlvSocketError;
use dataplane::{ReplicaKey, api::RequestMessage};
use fluvio_types::{SpuId};
use fluvio_storage::FileReplica;
use fluvio_controlplane_metadata::spu::SpuSpec;

use crate::{replication::leader::UpdateOffsetRequest, core::SharedSpuConfig};
use crate::services::internal::FetchStreamRequest;
use crate::core::spus::SharedSpuLocalStore;

use super::{FollowersState, state::FollowersBySpu};
use super::state::{SharedFollowersState, FollowerReplicaState};
use super::api_key::{FollowerPeerApiEnum};
use super::sync::{DefaultSyncRequest};
use super::peer_api::FollowerPeerRequest;

/// time to resync follower offsets to leader
const LEADER_RECONCILIATION_INTERVAL_SEC: u64 = 60; // 1 min

/// Controller for managing follower replicas
/// There is a controller for follower groups (group by leader SPU)
pub struct ReplicaFollowerController<S> {
    leader: SpuId,
    spus: SharedSpuLocalStore,
    states: SharedFollowersState<S>,
    config: SharedSpuConfig,
    spu_ctx: Arc<FollowersBySpu>,
}

impl ReplicaFollowerController<FileReplica> {
    pub fn run(
        leader: SpuId,
        spus: SharedSpuLocalStore,
        states: SharedFollowersState<FileReplica>,
        spu_ctx: Arc<FollowersBySpu>,
        config: SharedSpuConfig,
    ) {
        let controller = Self {
            leader,
            spus,
            states,
            spu_ctx,
            config,
        };
        spawn(controller.dispatch_loop());
    }

    fn local_spu_id(&self) -> SpuId {
        self.config.id()
    }

    #[instrument(
        name = "FollowerController",
        skip(self),
        fields (
            leader = self.leader
        )
    )]
    async fn dispatch_loop(mut self) {
        loop {
            let socket = self.create_socket_to_leader().await;

            match self.sync_with_leader(socket).await {
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
        }
        debug!("shutting down");
    }

    async fn sync_with_leader(&mut self, mut socket: FlvSocket) -> Result<bool, FlvSocketError> {
        let mut replicas = ReplicasBySpu::default();

        self.send_fetch_stream_request(&mut socket).await?;

        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<FollowerPeerRequest, FollowerPeerApiEnum>();

        let mut event_listener = self.spu_ctx.events.change_listner();

        loop {
            debug!("waiting request from leader");

            select! {
                _ = (sleep(Duration::from_secs(LEADER_RECONCILIATION_INTERVAL_SEC))) => {
                    debug!("timer fired - kickoff sync offsets to leader");
                    self.sync_all_offsets_to_leader(&mut sink,&replicas).await?;
                },

                _ = event_listener.listen() => {
                    // if out sync counter changes, then we need to re-compute replicas and send offsets again
                    replicas = ReplicasBySpu::filter_from(&self.states,self.leader);
                    self.sync_all_offsets_to_leader(&mut sink,&replicas).await?;
                }


                api_msg = api_stream.next() => {
                    if let Some(req_msg_res) = api_msg {
                        let req_msg = req_msg_res?;

                        match req_msg {
                            FollowerPeerRequest::SyncRecords(sync_request) => self.write_to_follower_replica(&mut sink,sync_request.request).await?,
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
            if let Some(spu) = self.spus.spec(&self.leader) {
                return spu;
            }

            debug!("leader spu spec is not available, waiting 1 second");
            sleep(Duration::from_millis(1000)).await;
            debug!("awake from sleep, checking spus");
        }
    }

    async fn write_to_follower_replica(
        &self,
        sink: &mut FlvSink,
        req: DefaultSyncRequest,
    ) -> Result<(), FlvSocketError> {
        debug!("received sync req");
        let offsets = self.write_topics(req).await;
        self.send_offsets_to_leader(sink, offsets).await
    }

    /// connect to leader, if can't connect try until we succeed
    /// or if we received termination message
    async fn create_socket_to_leader(&mut self) -> FlvSocket {
        let leader_spu = self.get_spu().await;
        let leader_endpoint = leader_spu.private_endpoint.to_string();

        let mut counter = 0;
        loop {
            debug!(
                %leader_endpoint,
                counter,
                "trying to create socket to leader",
            );

            match FlvSocket::connect(&leader_endpoint).await {
                Ok(socket) => {
                    debug!("connected to leader");
                    return socket;
                }

                Err(err) => {
                    error!("error connecting err: {}, sleeping ", err);
                    debug!("sleeping 1 seconds to connect to leader");
                    sleep(Duration::from_secs(5)).await;
                    counter += 1;
                }
            }
        }
    }

    /// establish stream to leader SPU
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

    /*
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
    */

    /// write records from leader to follower replica
    /// return updated offsets
    pub(crate) async fn write_topics(&self, mut req: DefaultSyncRequest) -> UpdateOffsetRequest {
        let mut offsets = UpdateOffsetRequest::default();

        for topic_request in &mut req.topics {
            let topic = &topic_request.name;
            for partition_request in &mut topic_request.partitions {
                let rep_id = partition_request.partition;
                let replica_key = ReplicaKey::new(topic.clone(), rep_id);
                if let Some(replica) = self.states.get(&replica_key) {
                    match replica
                        .write_recordsets(&mut partition_request.records)
                        .await
                    {
                        Ok(valid_record) => {
                            if valid_record {
                                let follow_leo = replica.leo();
                                let leader_hw = partition_request.hw;
                                debug!(follow_leo, leader_hw, "finish writing");
                                if follow_leo == leader_hw {
                                    debug!("follow leo and leader hw is same, updating hw");
                                    if let Err(err) = replica.update_hw(leader_hw).await {
                                        error!("error writing replica high watermark: {}", err);
                                    }
                                }

                                offsets.replicas.push(replica.as_offset_request());
                            }
                        }
                        Err(err) => error!(
                            "problem writing replica: {}, error: {:#?}",
                            replica_key, err
                        ),
                    }
                } else {
                    error!(
                        "unable to find follower replica for writing: {}",
                        replica_key
                    );
                }
            }
        }
        offsets
    }

    async fn sync_all_offsets_to_leader(
        &self,
        sink: &mut FlvSink,
        spu_replicas: &ReplicasBySpu,
    ) -> Result<(), FlvSocketError> {
        self.send_offsets_to_leader(sink, spu_replicas.replica_offsets())
            .await
    }

    /// send offset to leader
    async fn send_offsets_to_leader(
        &self,
        sink: &mut FlvSink,
        offsets: UpdateOffsetRequest,
    ) -> Result<(), FlvSocketError> {
        let follower_id = self.config.id();

        let req_msg = RequestMessage::new_request(offsets)
            .set_client_id(format!("follower: {}", follower_id));

        trace!(?req_msg, "sending offsets leader");

        sink.send_request(&req_msg).await
    }
}

/// replicas by spu which is used by follows controller
#[derive(Default)]
struct ReplicasBySpu(HashMap<ReplicaKey, FollowerReplicaState<FileReplica>>);

impl ReplicasBySpu {
    /// filter followers from followers state
    fn filter_from(states: &FollowersState<FileReplica>, leader: SpuId) -> Self {
        let mut replicas = HashMap::new();

        for rep_ref in states.iter() {
            if rep_ref.value().leader() == &leader {
                replicas.insert(rep_ref.key().clone(), rep_ref.value().clone());
            }
        }

        debug!(replica_count = replicas.len(), "compute replicas");

        Self(replicas)
    }

    // generate offset requests
    fn replica_offsets(&self) -> UpdateOffsetRequest {
        let replicas = self
            .0
            .values()
            .map(|replica| replica.as_offset_request())
            .collect();

        UpdateOffsetRequest { replicas }
    }

    /// send offset to leader
    #[allow(unused)]
    async fn send_offsets_to_leader(
        &self,
        follower: SpuId,
        sink: &mut FlvSink,
    ) -> Result<(), FlvSocketError> {
        let request = self.replica_offsets();

        let req_msg =
            RequestMessage::new_request(request).set_client_id(format!("follower: {}", follower));

        trace!(?req_msg, "sending offsets leader");

        sink.send_request(&req_msg).await
    }
}
