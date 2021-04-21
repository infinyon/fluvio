use std::fmt;

use tracing::{debug, error, warn};
use futures_util::stream::StreamExt;
use tracing::instrument;

use fluvio_storage::OffsetInfo;
use fluvio_socket::{FlvSink, FlvSocketError, FlvStream};
use dataplane::api::RequestMessage;
use fluvio_types::SpuId;

use crate::{
    core::DefaultSharedGlobalContext,
    replication::follower::sync::{
        FileSyncRequest, PeerFileTopicResponse, PeerFilePartitionResponse,
    },
};

use super::LeaderPeerApiEnum;
use super::LeaderPeerRequest;
use super::UpdateOffsetRequest;
use super::spu::SharedSpuPendingUpdate;

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
        sink: FlvSink,
        stream: FlvStream,
    ) {
        let connection = Self {
            ctx: ctx.clone(),
            max_bytes: ctx.config().peer_max_bytes,
            follower_id,
            spu_update,
        };

        connection.dispatch(sink, stream).await;
    }

    #[instrument(name = "LeaderConnection",skip(stream))]
    async fn dispatch(mut self, sink: FlvSink, stream: FlvStream) {
        if let Err(err) = self.inner_loop(sink, stream).await {
            error!("processing follower: {:#?}, terminating", err);
        }
    }

    async fn inner_loop(
        &mut self,
        mut sink: FlvSink,
        mut stream: FlvStream,
    ) -> Result<(), FlvSocketError> {
        use tokio::select;

        let mut api_stream = stream.api_stream::<LeaderPeerRequest, LeaderPeerApiEnum>();

        let mut listener = self.spu_update.listener();

        loop {
            debug!("waiting for next event");

            select! {

                _ = listener.listen() => {
                    debug!("hw has been updated");

                    self.update_hw_from_other(&mut sink).await?;
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
                                    self.handle_offset_request(request.request,&mut sink).await?;
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

    // updates form other SPU trigger this
    #[instrument(skip(self))]
    async fn update_hw_from_other(&mut self, sink: &mut FlvSink) -> Result<(), FlvSocketError> {
        let replicas = self.spu_update.dain_replicas().await;

        if replicas.is_empty() {
            debug!("no replicas. skipping");
            return Ok(());
        }

        let mut sync_request = FileSyncRequest::default();
        let leaders = self.ctx.leaders_state();

        for replica in replicas {
            if let Some(leader) = leaders.get(&replica) {
                let mut topic_response = PeerFileTopicResponse {
                    name: replica.topic.to_owned(),
                    ..Default::default()
                };

                let mut partition_response = PeerFilePartitionResponse {
                    partition: replica.partition,
                    ..Default::default()
                };
                let offset = leader.as_offset();
                debug!(
                    hw = offset.hw,
                    leo = offset.leo,
                    %replica,
                    "will sending hw to follower");
                // ensure leo and hw are set correctly. storage might have update last stable offset
                partition_response.leo = offset.leo;
                partition_response.hw = offset.hw;
                topic_response.partitions.push(partition_response);
                sync_request.topics.push(topic_response);
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
                .set_client_id(format!("leader hw update"));
            debug!("sending hw requests");
            sink.send_request(&request).await?;
            debug!("all hw send completed");
        }
        Ok(())
    }

    /// update each leader
    async fn handle_offset_request(
        &self,
        request: UpdateOffsetRequest,
        sink: &mut FlvSink,
    ) -> Result<(), FlvSocketError> {
        for update in request.replicas {
            let replica_key = update.replica;

            if let Some(leader) = self.ctx.leaders_state().get(&replica_key) {
                if leader
                    .value()
                    .update_states_from_followers(
                        self.follower_id,
                        OffsetInfo {
                            hw: update.hw,
                            leo: update.leo,
                        },
                    )
                    .await
                {
                    debug!("leader state change occur, need to send back to followers");
                    // if success we need to compute updates
                    let updates = leader.follower_updates().await;
                    if updates.is_empty() {
                        debug!(%replica_key,"no updates, do nothing");
                    } else {
                        for (follower, offset_update) in updates {
                            // our changes
                            if follower == self.follower_id {
                                leader
                                    .value()
                                    .send_update_to_follower(
                                        sink,
                                        self.follower_id,
                                        &offset_update,
                                        self.max_bytes,
                                    )
                                    .await?;
                            } else {
                                debug!(
                                    follower,
                                    %replica_key,
                                    "notifying other follower");
                                // notify followers that replica's hw need to be propogated
                                self.ctx
                                    .follower_updates()
                                    .update_hw(&follower, replica_key.clone())
                                    .await;
                            }
                        }
                    }
                }
            } else {
                error!(%replica_key,"no such replica");
            }
        }
        Ok(())
    }
}
