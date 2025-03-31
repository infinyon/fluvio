use std::{collections::HashMap, sync::Arc, time::Duration};

use tracing::{debug, error, trace, warn, instrument};
use async_lock::RwLock;
use adaptive_backoff::prelude::*;

use fluvio_types::SpuId;
use fluvio_types::event::offsets::OffsetPublisher;
use crate::core::FileGlobalContext;

use super::FollowersState;
use super::state::{SharedFollowersState, FollowerReplicaState};
use super::api_key::FollowerPeerApiEnum;
use super::sync::DefaultSyncRequest;
use super::peer_api::FollowerPeerRequest;

/// time to resync follower offsets to leader
const LEADER_RECONCILIATION_INTERVAL_SEC: u64 = 60; // 1 min

#[derive(Debug)]
pub struct FollowerGroups(RwLock<HashMap<SpuId, Arc<GroupNotification>>>);

impl FollowerGroups {
    pub fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// new follower replica has been added in the group
    /// ensure that controller exists if not spawn controller
    pub async fn check_new(&self, ctx: &FileGlobalContext, leader: SpuId) {
        // check if leader controller exist
        let mut leaders = self.0.write().await;
        // check if we have controllers
        if let Some(leaders) = leaders.get(&leader) {
            leaders.sync();
        } else {
            // don't have leader, so we need to create new one
            let notification = GroupNotification::shared();
            if let Some(old_notification) = leaders.insert(leader, notification.clone()) {
                old_notification.shutdown();
            } else {
                FollowGroupController::run(
                    leader,
                    ctx.spu_localstore_owned(),
                    ctx.followers_state_owned(),
                    notification,
                    ctx.config_owned(),
                );
            }
        }
    }

    /// remove
    pub async fn remove(&self, leader: SpuId) {
        let mut leaders = self.0.write().await;
        debug!("no more replica for leader, removing it");
        if let Some(old_leader) = leaders.remove(&leader) {
            debug!(leader, "more more replicas, shutting down");
            old_leader.shutdown();
        } else {
            error!(leader, "was not found");
        }
    }

    pub async fn update(&self, leader: SpuId) {
        let leaders = self.0.read().await;
        if let Some(old_leader) = leaders.get(&leader) {
            debug!(leader, "resync");
            old_leader.sync();
        } else {
            error!(leader, "was not found");
        }
    }
}

use inner::*;
mod inner {

    use tracing::info;
    use tokio::select;
    use futures_util::StreamExt;
    use once_cell::sync::Lazy;

    use fluvio_future::task::spawn;
    use fluvio_future::timer::sleep;
    use fluvio_socket::FluvioSocket;
    use fluvio_socket::FluvioSink;
    use fluvio_socket::SocketError;
    use fluvio_protocol::record::ReplicaKey;
    use fluvio_protocol::api::RequestMessage;
    use fluvio_types::SpuId;
    use fluvio_storage::FileReplica;
    use fluvio_controlplane_metadata::spu::SpuSpec;

    use crate::{replication::leader::UpdateOffsetRequest, core::SharedSpuConfig};
    use crate::services::internal::FetchStreamRequest;
    use crate::core::spus::SharedSpuLocalStore;

    static SHORT_RECONCILLATION: Lazy<u64> = Lazy::new(|| {
        let var_value = std::env::var("FLV_SHORT_RECONCILLATION").unwrap_or_default();
        var_value.parse().unwrap_or(10)
    });

    use super::*;

    /// Controller for managing follower replicas
    /// There is a controller for follower groups (group by leader SPU)
    pub struct FollowGroupController {
        leader: SpuId,
        spus: SharedSpuLocalStore,
        states: SharedFollowersState<FileReplica>,
        config: SharedSpuConfig,
        group: Arc<GroupNotification>,
    }

    impl FollowGroupController {
        pub fn run(
            leader: SpuId,
            spus: SharedSpuLocalStore,
            states: SharedFollowersState<FileReplica>,
            spu_ctx: Arc<GroupNotification>,
            config: SharedSpuConfig,
        ) {
            let controller = Self {
                leader,
                spus,
                states,
                group: spu_ctx,
                config,
            };
            spawn(controller.dispatch_loop());
        }

        fn local_spu_id(&self) -> SpuId {
            self.config.id()
        }

        #[instrument(
        skip(self),
        name = "FollowerGroupController",
        fields(
            leader = self.leader
        )
        )]
        async fn dispatch_loop(mut self) {
            let mut backoff = ExponentialBackoffBuilder::default()
                .min(Duration::from_secs(1))
                .max(Duration::from_secs(300))
                .build()
                .unwrap();

            loop {
                if self.group.is_end() {
                    debug!("end");
                    break;
                }

                let socket = self.create_socket_to_leader(&mut backoff).await;

                match self.sync_with_leader(socket).await {
                    Ok(terminate_flag) => {
                        if terminate_flag {
                            debug!("end command has received, terminating connection to leader");
                            break;
                        }
                    }
                    Err(err) => error!("err sync leader: {}", err),
                }

                warn!("lost connection to leader, sleeping 5 seconds and will retry it");

                if self.group.is_end() {
                    debug!("end");
                    break;
                }

                // 5 seconds is heuristic value, may change in the future or could be dynamic
                // depends on back off algorithm
                sleep(Duration::from_secs(5)).await;
            }
            debug!("shutting down");
        }

        async fn sync_with_leader(&mut self, socket: FluvioSocket) -> Result<bool, SocketError> {
            let (mut sink, mut stream) = socket.split();
            let mut api_stream = stream.api_stream::<FollowerPeerRequest, FollowerPeerApiEnum>();

            let mut event_listener = self.group.events.change_listener();

            // starts initial sync
            debug!("performing initial offset sync to leader");
            self.sync_all_offsets_to_leader(&mut sink).await?;

            let mut counter: i32 = 0;

            let mut timer = sleep(Duration::from_secs(LEADER_RECONCILIATION_INTERVAL_SEC));

            loop {
                debug!(counter, "waiting request from leader");

                select! {
                    _ = &mut timer => {
                        debug!("timer fired - kickoff sync offsets to leader");
                        self.sync_all_offsets_to_leader(&mut sink).await?;
                        timer= sleep(Duration::from_secs(LEADER_RECONCILIATION_INTERVAL_SEC));
                    },

                    offset_value = event_listener.listen() => {
                        if offset_value == -1 {
                            debug!("terminate signal");
                            return Ok(true);
                        }
                        self.sync_all_offsets_to_leader(&mut sink).await?;
                    }


                    api_msg = api_stream.next() => {
                        if let Some(req_msg_res) = api_msg {

                            match req_msg_res {
                                Ok(req_msg) => {
                                    match req_msg {
                                        FollowerPeerRequest::SyncRecords(sync_request)=> {
                                            info!(%self.leader, "sync records from leader!!");
                                            self.sync_from_leader(&mut sink,sync_request.request).await?},
                                         FollowerPeerRequest::RejectedOffsetRequest(requests) => {
                                             info!(fail_req = ?requests,"leader rejected these requests");
                                             timer= sleep(Duration::from_secs(*SHORT_RECONCILLATION));
                                         },
                                     }
                                }
                                Err(err) => {
                                    error!("error decoding req, terminating: {}", err);
                                    return Ok(false);
                                }
                            }
                        } else {
                            info!("leader socket has terminated");
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

                debug!("leader spu spec is not available, waiting 5 second");
                sleep(Duration::from_millis(5000)).await;
                debug!("awake from sleep, checking spus");
            }
        }

        #[instrument(skip(self, req))]
        async fn sync_from_leader(
            &self,
            sink: &mut FluvioSink,
            mut req: DefaultSyncRequest,
        ) -> Result<(), SocketError> {
            let mut offsets = UpdateOffsetRequest::default();

            info!(%self.leader, "syncing from leader, with {} topics", req.topics.len());
            for topic_request in &mut req.topics {
                info!(%topic_request.name, "syncing topic");
                let topic = &topic_request.name;
                for p in &mut topic_request.partitions {
                    info!(%topic, %p.partition, "syncing partition");
                    let rep_id = p.partition;
                    let replica_key = ReplicaKey::new(topic.clone(), rep_id);
                    info!(
                    replica = %replica_key,
                    leader_hw=p.hw,
                    leader_leo=p.leo,
                    records = p.records.total_records(),
                    base_offset = p.records.base_offset(),
                    "update from leader");
                    if let Some(replica) = self.states.get(&replica_key).await {
                        match replica.update_from_leader(&mut p.records, p.hw).await {
                            Ok(changes) => {
                                if changes {
                                    info!("changes occur, need to send back offset");
                                    offsets.replicas.push(replica.as_offset_request());
                                } else {
                                    info!("no changes");
                                }
                            }
                            Err(err) => {
                                error!("problem updating {}, error: {:#?}", replica_key, err)
                            }
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
        async fn create_socket_to_leader(
            &mut self,
            backoff: &mut ExponentialBackoff,
        ) -> FluvioSocket {
            let leader_spu = self.get_spu().await;
            let leader_endpoint = leader_spu.private_endpoint.to_string();

            let mut counter = 0;
            loop {
                debug!(
                    %leader_endpoint,
                    counter,
                    "trying connect to leader",
                );

                match FluvioSocket::connect(&leader_endpoint).await {
                    Ok(mut socket) => {
                        info!("connected to leader");

                        match self.send_fetch_stream_request(&mut socket).await {
                            Ok(Some(spu)) => {
                                info!("connected to leader with spu {} as replica", spu);
                                return socket;
                            }
                            Ok(None) => {
                                error!("leader rejected connection");
                                drop(socket);
                            }
                            Err(err) => {
                                error!("error stabilishing fetch stream: {}", err);
                                drop(socket);
                            }
                        }
                    }
                    Err(err) => {
                        error!(
                            "error connecting to leader at: <{}> err: {}",
                            leader_endpoint, err
                        );
                    }
                }

                let wait = backoff.wait();
                info!(
                    seconds = wait.as_secs(),
                    "sleeping seconds to connect to leader"
                );
                sleep(wait).await;
                counter += 1;
            }
        }

        /// establish stream to leader SPU
        #[instrument(skip(self))]
        async fn send_fetch_stream_request(
            &self,
            socket: &mut FluvioSocket,
        ) -> Result<Option<SpuId>, SocketError> {
            let local_spu_id = self.local_spu_id();
            info!("sending fetch stream for leader",);
            let max_bytes = self.config.peer_max_bytes as i32;
            let fetch_request = FetchStreamRequest {
                spu_id: local_spu_id,
                leader_spu_id: self.leader,
                max_bytes,
                ..Default::default()
            };
            let mut message = RequestMessage::new_request(fetch_request);
            message
                .get_mut_header()
                .set_client_id(format!("peer spu: {local_spu_id}"));

            let response = socket.send(&message).await?;
            trace!(?response, "follower: fetch stream response",);
            info!("follower: established peer to peer channel to leader",);
            Ok(response.response.spu_id)
        }

        async fn sync_all_offsets_to_leader(
            &self,
            sink: &mut FluvioSink,
        ) -> Result<(), SocketError> {
            let spu_replicas = FollowerGroup::filter_from(&self.states, self.leader).await;
            info!(
                %self.leader,
                "syncing all offsets to leader {:?}", spu_replicas.replica_offsets()
            );
            let a = self
                .send_offsets_to_leader(sink, spu_replicas.replica_offsets())
                .await;

            match a {
                Ok(_) => {
                    info!("all offsets sent to leader");
                    Ok(())
                }
                Err(err) => {
                    error!("error sending all offsets to leader: {}", err);
                    Err(err)
                }
            }
        }

        /// send offset to leader
        #[instrument(skip(self))]
        async fn send_offsets_to_leader(
            &self,
            sink: &mut FluvioSink,
            offsets: UpdateOffsetRequest,
        ) -> Result<(), SocketError> {
            let local_spu = self.config.id();
            info!(local_spu, "sending offsets to leader");
            let req_msg = RequestMessage::new_request(offsets)
                .set_client_id(format!("follower spu: {local_spu}"));

            match sink.send_request(&req_msg).await {
                Ok(_) => {
                    info!("offsets sent to leader");
                    Ok(())
                }
                Err(err) => {
                    error!("error sending offsets to leader: {}", err);
                    Err(err)
                }
            }
        }
    }

    /// replicas by spu which is used by follows controller
    #[derive(Default)]
    struct FollowerGroup(HashMap<ReplicaKey, FollowerReplicaState<FileReplica>>);

    impl FollowerGroup {
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

    /// Used to communicate changes to Group Controller
    #[derive(Debug)]
    pub struct GroupNotification {
        pub events: Arc<OffsetPublisher>,
    }

    impl GroupNotification {
        pub fn shared() -> Arc<Self> {
            Arc::new(Self {
                events: Arc::new(OffsetPublisher::new(0)),
            })
        }

        /// update count by 1 to force controller to re-compute replicas in it's holding
        pub fn sync(&self) {
            let last_value = self.events.current_value();
            self.events.update(last_value + 1);
        }

        pub fn shutdown(&self) {
            self.events.update(-1);
        }

        fn is_end(&self) -> bool {
            self.events.current_value() == -1
        }
    }
}
