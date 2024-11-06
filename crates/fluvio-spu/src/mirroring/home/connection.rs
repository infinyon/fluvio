use std::time::Duration;
use std::{fmt, sync::Arc};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use tokio::select;
use tracing::{debug, error, info, instrument, warn};
use anyhow::{Result, anyhow};
use futures_util::StreamExt;

use fluvio_auth::{AuthContext, InstanceAction};
use fluvio_controlplane_metadata::mirror::{MirrorPairStatus, MirrorType};
use fluvio_controlplane_metadata::extended::ObjectType;
use fluvio_future::timer::sleep;
use fluvio_protocol::{record::Offset, api::RequestMessage};
use fluvio_spu_schema::server::mirror::StartMirrorRequest;
use fluvio_socket::{ExclusiveFlvSink, FluvioStream};
use fluvio::Isolation;

use crate::control_plane::SharedMirrorStatusUpdate;
use crate::core::DefaultSharedGlobalContext;
use crate::mirroring::remote::api_key::MirrorRemoteApiEnum;
use crate::mirroring::remote::remote_api::RemoteMirrorRequest;
use crate::mirroring::remote::sync::{DefaultRemotePartitionSyncRequest, MirrorPartitionSyncRequest};
use crate::mirroring::remote::update_offsets::UpdateRemoteOffsetRequest;
use crate::replication::leader::SharedFileLeaderState;
use crate::services::auth::SpuAuthServiceContext;

use super::sync::HomeFilePartitionSyncRequest;
use super::update_offsets::UpdateHomeOffsetRequest;

const MIRROR_RECONCILIATION_INTERVAL_SEC: u64 = 60; // 1 min

const UNKNOWN_LEO: i64 = -1;

pub(crate) struct MirrorRequestMetrics {
    loop_count: AtomicU64,
    remote_leo: AtomicI64,
}

impl MirrorRequestMetrics {
    pub(crate) fn new() -> Self {
        Self {
            loop_count: AtomicU64::new(0),
            remote_leo: AtomicI64::new(UNKNOWN_LEO),
        }
    }

    fn increase_loop_count(&self) {
        self.loop_count.fetch_add(1, Ordering::Relaxed);
    }

    fn get_loop_count(&self) -> u64 {
        self.loop_count.load(Ordering::Relaxed)
    }

    fn get_remote_leo(&self) -> i64 {
        self.remote_leo.load(Ordering::SeqCst)
    }

    fn update_remote_leo(&self, leo: Offset) {
        self.remote_leo.store(leo, Ordering::SeqCst);
    }
}

/// Handle mirror request from remote
pub(crate) struct MirrorHomeHandler {
    metrics: Arc<MirrorRequestMetrics>,
    /// leader replicat that will be mirrored
    leader: SharedFileLeaderState,
    ctx: DefaultSharedGlobalContext,
    status_update: SharedMirrorStatusUpdate,
    remote_cluster_id: String,
}

impl fmt::Debug for MirrorHomeHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MirrorRequestHandler")
    }
}

impl MirrorHomeHandler {
    /// start handling mirror request sync from remote
    /// it is called from public service handler
    pub(crate) async fn respond<AC: AuthContext>(
        req_msg: RequestMessage<StartMirrorRequest>,
        auth_ctx: &SpuAuthServiceContext<AC>,
        sink: ExclusiveFlvSink,
        stream: FluvioStream,
    ) {
        let mirror_status_update = auth_ctx.global_ctx.mirror_status_update_owned();
        // authorization check
        if let Ok(authorized) = auth_ctx
            .auth
            .allow_instance_action(
                ObjectType::Mirror,
                InstanceAction::Update,
                &req_msg.request.remote_cluster_id,
            )
            .await
        {
            if !authorized {
                warn!(
                    "identity mismatch for remote_id: {}",
                    req_msg.request.remote_cluster_id
                );
                return;
            }
        }

        // check if remote cluster exists
        let mirrors = auth_ctx.global_ctx.mirrors_localstore().all_values();
        let remote = mirrors
            .iter()
            .find(|mirror| match &mirror.spec.mirror_type {
                MirrorType::Remote(r) => r.id == req_msg.request.remote_cluster_id,
                _ => false,
            });

        if remote.is_none() {
            warn!(
                "remote cluster not found: {}",
                req_msg.request.remote_cluster_id
            );
            return;
        }

        debug!("handling mirror request: {:#?}", req_msg);
        let remote_replica = req_msg.request.remote_replica;
        let remote_cluster_id = req_msg.request.remote_cluster_id;

        if let Some((leader, source)) = auth_ctx
            .global_ctx
            .leaders_state()
            .find_mirror_home_leader(&remote_cluster_id, &remote_replica)
            .await
        {
            debug!(leader = %leader.id(), "found leader replica for this mirror request");
            // map to actual home
            let metrics = Arc::new(MirrorRequestMetrics::new());

            let handler: MirrorHomeHandler = Self {
                metrics: metrics.clone(),
                leader,
                ctx: auth_ctx.global_ctx.clone(),
                status_update: mirror_status_update.clone(),
                remote_cluster_id: remote_cluster_id.clone(),
            };

            if source {
                if let Err(err) = handler.respond_as_source(sink, stream).await {
                    error!("error handling mirror request: {:#?}", err);

                    if let Err(err) = mirror_status_update
                        .send_status(
                            remote_cluster_id.clone(),
                            MirrorPairStatus::DetailFailure(err.to_string()),
                        )
                        .await
                    {
                        error!("error updating status: {}", err);
                    }
                }
            } else if let Err(err) = handler.respond_as_target(sink, stream).await {
                error!("error handling mirror request: {:#?}", err);

                if let Err(err) = mirror_status_update
                    .send_status(
                        remote_cluster_id.clone(),
                        MirrorPairStatus::DetailFailure(err.to_string()),
                    )
                    .await
                {
                    error!("error updating status: {}", err);
                }
            }
        } else {
            warn!(
                remote_replica,
                remote_cluster_id, "no leader replica found for this mirror request"
            );
        }
    }

    /// respond to mirror request from remote as target
    async fn respond_as_target(
        self,
        mut sink: ExclusiveFlvSink,
        mut stream: FluvioStream,
    ) -> Result<()> {
        // first send
        let mut api_stream = stream.api_stream::<RemoteMirrorRequest, MirrorRemoteApiEnum>();

        // TODO: Add delete event on replica.

        // send initial offset state of home
        self.send_offsets_to_remote(&mut sink).await?;

        // create timer
        let mut timer = sleep(Duration::from_secs(MIRROR_RECONCILIATION_INTERVAL_SEC));

        self.update_status(MirrorPairStatus::Successful).await?;

        loop {
            debug!(
                counter = self.metrics.get_loop_count(),
                "waiting for mirror request"
            );

            select! {
                _ = &mut timer => {
                    debug!("timer expired, sending reconciliation");
                    self.send_offsets_to_remote(&mut sink).await?;
                    timer = sleep(Duration::from_secs(MIRROR_RECONCILIATION_INTERVAL_SEC));
                },
                remote_msg = api_stream.next() => {
                    if let Some(req_msg_res) = remote_msg {
                        let req_msg = req_msg_res?;

                        match req_msg {
                            RemoteMirrorRequest::SyncRecords(sync_request)=> {
                                self.sync_record_from_remote(&mut sink,sync_request.request).await?;
                            }
                            RemoteMirrorRequest::UpdateRemoteOffset(_req) => {
                                return Err(anyhow!("received  offset request from remote, this should not happen, since we are target"));
                            }
                         }

                    } else {
                        self.update_status(MirrorPairStatus::DetailFailure("closed connection".to_owned())).await?;
                        debug!("leader socket has terminated");
                        break;
                    }
                }
            }

            self.metrics.increase_loop_count();
        }

        Ok(())
    }

    async fn update_status(&self, status: MirrorPairStatus) -> Result<()> {
        self.status_update
            .send_status(self.remote_cluster_id.clone(), status)
            .await
    }

    // send mirror home's offset to remote so it can synchronize
    async fn send_offsets_to_remote(&self, sink: &mut ExclusiveFlvSink) -> Result<()> {
        let offset_request = UpdateHomeOffsetRequest {
            replica: self.leader.id().clone(),
            leo: self.leader.leo(),
            hw: self.leader.hw(),
        };

        debug!("sending offset info: {:#?}", offset_request);
        let req_msg = RequestMessage::new_request(offset_request).set_client_id("mirror home");

        sink.send_request(&req_msg).await?;

        Ok(())
    }

    #[instrument(skip(self, sink, req))]
    async fn sync_record_from_remote(
        &self,
        sink: &mut ExclusiveFlvSink,
        mut req: DefaultRemotePartitionSyncRequest,
    ) -> Result<()> {
        let append_flag = self
            .leader
            .append_record_set(&mut req.records, self.ctx.follower_notifier())
            .await?;
        debug!(append_flag, "leader appended");
        self.send_offsets_to_remote(sink).await
    }

    /// respond to mirror request from remote as source
    async fn respond_as_source(
        self,
        sink: ExclusiveFlvSink,
        mut stream: FluvioStream,
    ) -> Result<()> {
        // first send
        let mut api_stream = stream.api_stream::<RemoteMirrorRequest, MirrorRemoteApiEnum>();

        // if remote is behind, we need to update remote
        let mut remote_updated_needed = false;

        let mut leader_offset_listener = self.leader.offset_listener(&Isolation::ReadUncommitted);

        #[allow(unused_assignments)]
        loop {
            let remote_leo = self.metrics.get_remote_leo();
            debug!(
                counter = self.metrics.get_loop_count(),
                remote_leo, remote_updated_needed, "waiting for mirror event"
            );

            // send missing records to remote if remote is behind

            if remote_updated_needed && remote_leo >= 0 {
                self.send_records_to_remote(&sink, remote_leo).await?;
                remote_updated_needed = false;
            }

            select! {
                _ = leader_offset_listener.listen() => {
                    info!("leader offset has changed, remote cluster needs to be updated");
                    remote_updated_needed = true;
                },


                remote_msg = api_stream.next() => {
                    if let Some(req_msg_res) = remote_msg {
                        let req_msg = req_msg_res?;

                        match req_msg {
                            RemoteMirrorRequest::SyncRecords(_sync_request)=> {
                                return Err(anyhow!("received sync request from remote, this should not happen, since we are source"));
                            }
                            RemoteMirrorRequest::UpdateRemoteOffset(req) => {
                                remote_updated_needed = self.update_from_remote(req)?;
                            }
                         }

                    } else {
                        self.update_status(MirrorPairStatus::DetailFailure("closed connection".to_owned())).await?;
                        debug!("leader socket has terminated");
                        break;
                    }
                }
            }

            self.metrics.increase_loop_count();
        }

        info!("remote has closed connection, terminating");

        Ok(())
    }

    #[instrument(skip(self, sink))]
    async fn send_records_to_remote(
        &self,
        sink: &ExclusiveFlvSink,
        remote_leo: Offset,
    ) -> Result<()> {
        debug!("updating home cluster");
        if let Some(sync_request) = self.generate_home_records_as_source(remote_leo).await? {
            debug!(?sync_request, "home sync");
            let request = RequestMessage::new_request(sync_request)
                .set_client_id(format!("leader: {}", self.leader.id()));

            let mut inner_sink = sink.lock().await;
            inner_sink
                .encode_file_slices(&request, request.header.api_version())
                .await?;
            Ok(())
        } else {
            Ok(())
        }
    }

    /// home is source, generate missing records to send to \remote
    async fn generate_home_records_as_source(
        &self,
        remote_leo: Offset,
    ) -> Result<Option<HomeFilePartitionSyncRequest>> {
        const MAX_BYTES: u32 = 1024 * 1024; // 1MB

        // leader off should be always greater than remote leo
        let leader_offset = self.leader.as_offset();

        // if remote mirror is all caught up, there is no need to send out update
        if leader_offset.leo == remote_leo {
            debug!("remote has caught up, just chilling out");
            return Ok(None);
        }

        let mut partition_response = MirrorPartitionSyncRequest {
            leo: leader_offset.leo,
            hw: leader_offset.hw,
            ..Default::default()
        };

        if leader_offset.leo > remote_leo {
            match self
                .leader
                .read_records(remote_leo, MAX_BYTES, Isolation::ReadUncommitted)
                .await
            {
                // leader offset is greater than home, we need to send records to home (default)
                Ok(slice) => {
                    debug!(
                        hw = slice.end.hw,
                        leo = slice.end.leo,
                        replica = %self.leader.id(),
                        "read records"
                    );
                    if let Some(file_slice) = slice.file_slice {
                        partition_response.records = file_slice.into();
                    }
                    Ok(Some(partition_response.into()))
                }
                Err(err) => {
                    error!(%err, "error reading records");
                    Err(anyhow!("error reading records: {}", err))
                }
            }
        } else {
            // home has more records, then we sync copy records from home
            debug!(
                hw = leader_offset.hw,
                leo = leader_offset.leo,
                remote_leo,
                "oh no mirror home has more records"
            );
            Err(anyhow!(
                "leader has more records than home, this should not happen"
            ))
        }
    }

    // received new offset from edge, this happens when edge is behind
    #[instrument(skip(req))]
    fn update_from_remote(&self, req: RequestMessage<UpdateRemoteOffsetRequest>) -> Result<bool> {
        let leader_leo = self.leader.leo();
        let old_remote_leo = self.metrics.get_remote_leo();
        let new_remote_leo = req.request.offset().leo;

        debug!(
            leader_leo,
            old_remote_leo, new_remote_leo, "received update from remote"
        );
        // if old home leo is not initialized, we need to update home
        if old_remote_leo < 0 {
            debug!(new_remote_leo, "updating remote leo from uninitialized");
            self.metrics.update_remote_leo(new_remote_leo);
        }

        // check how far remote is behind
        match new_remote_leo.cmp(&leader_leo) {
            std::cmp::Ordering::Greater => {
                // remote leo should never be greater than leader's leo
                warn!(
                    leader_leo,
                    new_remote_leo,
                    "remote has more records, this should not happen, this is error"
                );
                return Err(anyhow!("remote's leo: {new_remote_leo} > leader's leo: {leader_leo} this should not happen, this is error"));
            }
            std::cmp::Ordering::Less => {
                debug!(
                    new_remote_leo,
                    leader_leo, "remote has less records, need to refresh home"
                );
                self.metrics.update_remote_leo(new_remote_leo);
                Ok(true)
            }
            std::cmp::Ordering::Equal => {
                debug!(
                    new_remote_leo,
                    "remote has same records, no need to refresh home"
                );
                Ok(false)
            }
        }
    }
}
