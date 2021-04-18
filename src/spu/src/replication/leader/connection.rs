use fluvio_storage::OffsetInfo;
use tracing::{trace, debug, error};
use tracing::instrument;
use fluvio_socket::{FlvSocketError, FlvStream, FlvSocket, FlvSink};
use fluvio_service::api_loop;
use fluvio_types::SpuId;

use crate::core::DefaultSharedGlobalContext;

use super::LeaderReplicaControllerCommand;
use super::FollowerOffsetUpdate;
use super::LeaderPeerApiEnum;
use super::LeaderPeerRequest;
use super::UpdateOffsetRequest;
use super::ReplicaOffsetRequest;

/// Handle connection request from follower
/// This follows similar arch as Consumer Stream Fetch Handler
/// It is reactive to offset state
pub struct FollowerHandler {
    ctx: DefaultSharedGlobalContext,
    follower_id: SpuId,
    max_bytes: u32,
}

impl FollowerHandler {
    /// manage connection from follower
    pub async fn start(
        ctx: DefaultSharedGlobalContext,
        follower_id: SpuId,
        socket: FlvSocket,
    ) -> Result<(), FlvSocketError> {
        let connection = Self {
            ctx: ctx.clone(),
            max_bytes: ctx.config().peer_max_bytes,
            follower_id,
        };

        connection.main_loop(socket).await?;

        Ok(())
    }

    #[instrument(
        name = "LeaderConnection",
        skip(self,socket),
        fields(
            follow_id = %self.follower_id
        )
    )]
    async fn main_loop(&self, socket: FlvSocket) -> Result<(), FlvSocketError> {
        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<LeaderPeerRequest, LeaderPeerApiEnum>();

        api_loop!(
            api_stream,
            LeaderPeerRequest::UpdateOffsets(request) => {
                self.handle_offset_request(request.request,&mut sink).await?;
            }
        );

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
                    // if success we need to compute updates
                    let updates = leader.follower_updates().await;
                    if updates.is_empty() {
                        debug!(%replica_key,"no updates, do nothing");
                    } else {
                        for (spu, offset_update) in updates {
                            // this is for us we can sed
                            if spu == self.follower_id {
                                leader
                                    .value()
                                    .send_update_to_follower(
                                        sink,
                                        self.follower_id,
                                        &offset_update,
                                        self.max_bytes,
                                    )
                                    .await?;
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
