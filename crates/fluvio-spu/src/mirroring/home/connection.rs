use std::time::Duration;
use std::{fmt, sync::Arc};
use std::sync::atomic::AtomicU64;

use tokio::select;
use tracing::{debug, error, instrument, warn};
use anyhow::Result;

use fluvio_future::timer::sleep;
use fluvio_protocol::api::RequestMessage;
use fluvio_spu_schema::server::mirror::StartMirrorRequest;
use futures_util::StreamExt;
use fluvio_socket::{FluvioStream, ExclusiveFlvSink};

use crate::core::DefaultSharedGlobalContext;
use crate::mirroring::remote::api_key::MirrorRemoteApiEnum;
use crate::mirroring::remote::remote_api::RemoteMirrorRequest;
use crate::mirroring::remote::sync::DefaultPartitionSyncRequest;
use crate::replication::leader::SharedFileLeaderState;

use super::update_offsets::UpdateHomeOffsetRequest;

const MIRROR_RECONCILIATION_INTERVAL_SEC: u64 = 60; // 1 min

pub(crate) struct MirrorRequestMetrics {
    loop_count: AtomicU64,
}

impl MirrorRequestMetrics {
    pub(crate) fn new() -> Self {
        Self {
            loop_count: AtomicU64::new(0),
        }
    }

    fn increase_loop_count(&self) {
        self.loop_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn get_loop_count(&self) -> u64 {
        self.loop_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Handle mirror request from remote
pub(crate) struct MirrorHomeHandler {
    metrics: Arc<MirrorRequestMetrics>,
    leader: SharedFileLeaderState,
    ctx: DefaultSharedGlobalContext,
}

impl fmt::Debug for MirrorHomeHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MirrorRequestHandler")
    }
}

impl MirrorHomeHandler {
    /// start handling mirror request sync from remote
    /// it is called from public service handler
    pub(crate) async fn respond(
        ctx: DefaultSharedGlobalContext,
        req_msg: RequestMessage<StartMirrorRequest>,
        sink: ExclusiveFlvSink,
        stream: &mut FluvioStream,
    ) {
        debug!("handling mirror request: {:#?}", req_msg);
        let remote_replica = req_msg.request.remote_replica;
        let remote_cluster_id = req_msg.request.remote_cluster_id;
        let _access_key = req_msg.request.access_key;

        if let Some(leader) = ctx
            .leaders_state()
            .find_mirror_home_leader(&remote_cluster_id, &remote_replica)
            .await
        {
            debug!(leader = %leader.id(), "found leader replica for this mirror request");
            // map to actual home
            let metrics = Arc::new(MirrorRequestMetrics::new());

            // TODO: perform authorization
            let handler: MirrorHomeHandler = Self {
                metrics: metrics.clone(),
                leader,
                ctx,
            };

            if let Err(err) = handler.inner_respond(sink, stream).await {
                error!("error handling mirror request: {:#?}", err);
            }
        } else {
            // TODO: handle no home partition
            warn!(
                remote_replica,
                remote_cluster_id, "no leader replica found for this"
            );
        }
    }

    /// main respond handler
    async fn inner_respond(
        self,
        mut sink: ExclusiveFlvSink,
        stream: &mut FluvioStream,
    ) -> Result<()> {
        // first send
        let mut api_stream = stream.api_stream::<RemoteMirrorRequest, MirrorRemoteApiEnum>();

        // create timer
        let mut timer = sleep(Duration::from_secs(MIRROR_RECONCILIATION_INTERVAL_SEC));

        // TODO: Add delete event on replica.

        // send initial offset state of home
        self.send_offsets_to_remote(&mut sink).await?;

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
                         }

                    } else {
                        debug!("leader socket has terminated");
                        break;
                    }
                }
            }

            self.metrics.increase_loop_count();
        }

        Ok(())
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
        mut req: DefaultPartitionSyncRequest,
    ) -> Result<()> {
        let append_flag = self
            .leader
            .append_record_set(&mut req.records, self.ctx.follower_notifier())
            .await?;
        debug!(append_flag, "leader appended");
        self.send_offsets_to_remote(sink).await
    }
}
