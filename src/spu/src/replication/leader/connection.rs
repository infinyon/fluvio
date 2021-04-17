use fluvio_storage::OffsetInfo;
use tracing::{trace, error};
use tracing::instrument;
use fluvio_socket::{FlvSocketError, FlvStream, FlvSocket};
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
}

impl FollowerHandler {
    /// manage connection from follower
    pub async fn start(
        ctx: DefaultSharedGlobalContext,
        follower_id: SpuId,
        socket: FlvSocket,
    ) -> Result<(), FlvSocketError> {
        let (sink, stream) = socket.split();

        let connection = Self {
            ctx: ctx.clone(),
            follower_id,
        };

        connection.main_loop(stream).await?;

        Ok(())
    }

    #[instrument(
        name = "LeaderConnection",
        skip(self,stream),
        fields(
            follow_id = %self.follower_id
        )
    )]
    async fn main_loop(&self, mut stream: FlvStream) -> Result<(), FlvSocketError> {
        trace!(
            "starting connection handling from follower: {} for leader: {}",
            self.follower_id,
            self.ctx.local_spu_id()
        );

        let mut api_stream = stream.api_stream::<LeaderPeerRequest, LeaderPeerApiEnum>();

        api_loop!(
            api_stream,
            LeaderPeerRequest::UpdateOffsets(request) => {
                self.handle_offset_request(request.request).await
            }
        );

        Ok(())
    }

    /// update each leader
    async fn handle_offset_request(&self, request: UpdateOffsetRequest) {
        for update in request.replicas {
            let replica_key = update.replica;

            if let Some(leader) = self.ctx.leaders_state().get(&replica_key) {
                leader
                    .value()
                    .update_hw_from_followers(
                        self.follower_id,
                        OffsetInfo {
                            hw: update.hw,
                            leo: update.leo,
                        },
                    )
                    .await;
            } else {
                error!(%replica_key,"no such replica");
            }
        }
    }
}
