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
        fields(
            leader = self.leader,
            local = self.config.id()
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
        self.send_fetch_stream_request(&mut socket).await?;

        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<FollowerPeerRequest, FollowerPeerApiEnum>();

        let mut event_listener = self.spu_ctx.events.change_listner();

        // starts initial sync
        let mut replicas = ReplicasBySpu::filter_from(&self.states, self.leader).await;
        self.sync_all_offsets_to_leader(&mut sink, &replicas)
            .await?;

        let mut counter: i32 = 0;

        loop {
            debug!(counter, "waiting request from leader");

            select! {
                _ = (sleep(Duration::from_secs(LEADER_RECONCILIATION_INTERVAL_SEC))) => {
                    debug!("timer fired - kickoff sync offsets to leader");
                    self.sync_all_offsets_to_leader(&mut sink,&replicas).await?;
                },

                _ = event_listener.listen() => {
                    // if sync counter changes, then we need to re-compute replicas and send offsets again
                    replicas = ReplicasBySpu::filter_from(&self.states,self.leader).await;
                    self.sync_all_offsets_to_leader(&mut sink,&replicas).await?;
                }


                api_msg = api_stream.next() => {
                    if let Some(req_msg_res) = api_msg {
                        let req_msg = req_msg_res?;

                        match req_msg {
                            FollowerPeerRequest::SyncRecords(sync_request) => self.sync_from_leader(&mut sink,sync_request.request).await?,
                        }

                    } else {
                        debug!("leader socket has terminated");
                        return Ok(false);
                    }
                }
            }

            counter += 1;
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

    #[instrument(skip(self, req))]
    async fn sync_from_leader(
        &self,
        sink: &mut FlvSink,
        mut req: DefaultSyncRequest,
    ) -> Result<(), FlvSocketError> {
        let mut offsets = UpdateOffsetRequest::default();

        for topic_request in &mut req.topics {
            let topic = &topic_request.name;
            for p in &mut topic_request.partitions {
                let rep_id = p.partition;
                let replica_key = ReplicaKey::new(topic.clone(), rep_id);
                debug!(
                    replica = %replica_key,
                    hw=p.hw,
                    leo=p.leo,
                    records = p.records.total_records(),
                    base_offset = p.records.base_offset(),
                    "update from leader");
                if let Some(replica) = self.states.get(&replica_key).await {
                    match replica.update_from_leader(&mut p.records, p.hw).await {
                        Ok(changes) => {
                            if changes {
                                debug!("changes occur, need to send back offset");
                                offsets.replicas.push(replica.as_offset_request());
                            } else {
                                debug!("no changes");
                            }
                        }
                        Err(err) => error!("problem updating {}, error: {:#?}", replica_key, err),
                    }
                } else {
                    error!(
                        "unable to find follower replica for writing: {}",
                        replica_key
                    );
                }
            }
        }

        if !offsets.replicas.is_empty() {
            self.send_offsets_to_leader(sink, offsets).await
        } else {
            Ok(())
        }
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
    #[instrument(skip(self))]
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

    async fn sync_all_offsets_to_leader(
        &self,
        sink: &mut FlvSink,
        spu_replicas: &ReplicasBySpu,
    ) -> Result<(), FlvSocketError> {
        self.send_offsets_to_leader(sink, spu_replicas.replica_offsets())
            .await
    }

    /// send offset to leader
    #[instrument(skip(self))]
    async fn send_offsets_to_leader(
        &self,
        sink: &mut FlvSink,
        offsets: UpdateOffsetRequest,
    ) -> Result<(), FlvSocketError> {
        let local_spu = self.config.id();
        debug!(local_spu, "sending offsets to leader");
        let req_msg = RequestMessage::new_request(offsets)
            .set_client_id(format!("follower spu: {}", local_spu));

        sink.send_request(&req_msg).await
    }
}

/// replicas by spu which is used by follows controller
#[derive(Default)]
struct ReplicasBySpu(HashMap<ReplicaKey, FollowerReplicaState<FileReplica>>);

impl ReplicasBySpu {
    /// filter followers from followers state
    async fn filter_from(states: &FollowersState<FileReplica>, leader: SpuId) -> Self {
        let replicas = states.followers_by_spu(leader).await;
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
}
