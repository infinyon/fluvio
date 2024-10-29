use std::fmt;

use tracing::{debug, error, warn};
use futures_util::stream::StreamExt;
use tracing::instrument;

use fluvio_storage::OffsetInfo;
use fluvio_socket::{FluvioSink, SocketError, FluvioStream};
use fluvio_protocol::api::RequestMessage;
use fluvio_types::SpuId;

use crate::{core::DefaultSharedGlobalContext, replication::follower::sync::FileSyncRequest};

use super::LeaderPeerApiEnum;
use super::LeaderPeerRequest;
use super::UpdateOffsetRequest;
use super::spu::SharedSpuPendingUpdate;
use super::super::follower::RejectOffsetRequest;

/// Handle connection request from follower
/// This follows similar arch as Consumer Stream Fetch Handler
/// It is reactive to offset state
pub struct FollowerHandler {
    ctx: DefaultSharedGlobalContext,
    follower_id: SpuId,
    max_bytes: u32,
    spu_update: SharedSpuPendingUpdate,
}

impl fmt::Debug for FollowerHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.follower_id)
    }
}

impl FollowerHandler {
    /// manage connection from follower
    pub async fn start(
        ctx: DefaultSharedGlobalContext,
        follower_id: SpuId,
        spu_update: SharedSpuPendingUpdate,
        sink: FluvioSink,
        stream: FluvioStream,
    ) {
        let connection = Self {
            ctx: ctx.clone(),
            max_bytes: ctx.config().peer_max_bytes,
            follower_id,
            spu_update,
        };

        connection.dispatch(sink, stream).await;
    }

    #[instrument(name = "LeaderConnection", skip(stream))]
    async fn dispatch(mut self, sink: FluvioSink, stream: FluvioStream) {
        if let Err(err) = self.inner_loop(sink, stream).await {
            error!("processing follower: {:#?}, terminating", err);
        }
    }

    async fn inner_loop(
        &mut self,
        mut sink: FluvioSink,
        mut stream: FluvioStream,
    ) -> Result<(), SocketError> {
        use tokio::select;

        let mut api_stream = stream.api_stream::<LeaderPeerRequest, LeaderPeerApiEnum>();

        let mut listener = self.spu_update.listener();

        loop {
            debug!("waiting for next event");

            select! {

                _ = listener.listen() => {
                    debug!("this follower needs to be updated");

                    self.update_from_leaders(&mut sink).await?;
                },


                //
                // same as this
                //  api_loop!(
                //   api_stream,
                //     LeaderPeerRequest::UpdateOffsets(request) => {
                //     self.handle_offset_request(request.request,&mut sink).await?;
                //  )
                //  expanded to used in the select
                api_msg = api_stream.next() => {

                    if let Some(msg) = api_msg {
                        if let Ok(req_message) = msg {
                            match req_message {

                                LeaderPeerRequest::UpdateOffsets(request) => {
                                    self.update_from_follower(request.request,&mut sink).await?;
                                }
                            }
                        } else {
                            debug!("error decoding req, terminating");
                            break;
                        }

                    } else {
                        debug!("no more msg, end");
                        break;
                    }

                }

            }
        }

        debug!("closing");

        Ok(())
    }

    // send out any updates from other leaders to this followers
    #[instrument(skip(self))]
    async fn update_from_leaders(&mut self, sink: &mut FluvioSink) -> Result<(), SocketError> {
        let replicas = self.spu_update.drain_replicas().await;

        if replicas.is_empty() {
            debug!("no replicas. skipping");
            return Ok(());
        }

        debug!(?replicas);

        let mut sync_request = FileSyncRequest::default();
        let leaders = self.ctx.leaders_state();

        for replica in replicas {
            if let Some(leader) = leaders.get(&replica).await {
                if let Some(topic_response) = leader
                    .follower_updates(&self.follower_id, self.max_bytes)
                    .await
                {
                    sync_request.topics.push(topic_response);
                }
            } else {
                warn!(
                    %replica,
                    "no existent leader replica"
                )
            }
        }

        if sync_request.topics.is_empty() {
            debug!("no topics found, skipping");
        } else {
            let request = RequestMessage::new_request(sync_request)
                .set_client_id(format!("leader: {}", self.ctx.local_spu_id()));
            sink.encode_file_slices(&request, request.header.api_version())
                .await?;
        }
        Ok(())
    }

    /// process updates from followers
    #[instrument(skip(self, request))]
    async fn update_from_follower(
        &self,
        request: UpdateOffsetRequest,
        sink: &mut FluvioSink,
    ) -> Result<(), SocketError> {
        let mut rejects = vec![];
        for update in request.replicas.into_iter() {
            debug!(?update, "request");
            let replica_key = update.replica;
            if let Some(leader) = self.ctx.leaders_state().get(&replica_key).await {
                let status = leader
                    .update_states_from_followers(
                        self.follower_id,
                        OffsetInfo {
                            hw: update.hw,
                            leo: update.leo,
                        },
                        self.ctx.follower_notifier(),
                    )
                    .await;
                debug!(status, replica = %leader.id(), "leader updated");
            } else {
                // if we didn't find it replica that means leader doesn't have upto date replicas.
                // we need to send back
                warn!(%replica_key,"no such replica");
                rejects.push(replica_key.clone());
            }
        }

        if !rejects.is_empty() {
            debug!(reject_count = rejects.len());
            let request = RequestMessage::new_request(RejectOffsetRequest { replicas: rejects })
                .set_client_id(format!("leader: {}", self.ctx.local_spu_id()));

            sink.send_request(&request).await?;
        }

        Ok(())
    }
}
