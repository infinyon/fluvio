use tracing::{trace, error, debug};
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

/// Handle connection from follower to leader
pub struct LeaderConnection {
    ctx: DefaultSharedGlobalContext,
    follower_id: SpuId,
}

impl LeaderConnection {
    /// manage connection from follower
    pub async fn handle(
        ctx: DefaultSharedGlobalContext,
        follower_id: SpuId,
        socket: FlvSocket,
    ) -> Result<(), FlvSocketError> {
        let (sink, stream) = socket.split();
        ctx.follower_sinks().insert_sink(follower_id, sink);

        let connection = Self {
            ctx: ctx.clone(),
            follower_id,
        };

        connection.main_loop(stream).await?;

        ctx.follower_sinks().clear_sink(&follower_id);
        Ok(())
    }

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
                self.route_offset_request(request.request).await
            }
        );

        Ok(())
    }

    /// route offset update request from follower to replica leader controller
    async fn route_offset_request(&self, request: UpdateOffsetRequest) {
        debug!("receive offset request from follower: {}", self.follower_id);
        for replica in request.replicas {
            route_replica_offset(self.ctx.clone(), self.follower_id, replica).await
        }
    }
}

/// send route replica offsets to leader replica controller
/// it spawn request
async fn route_replica_offset(
    ctx: DefaultSharedGlobalContext,
    follower_id: SpuId,
    replica: ReplicaOffsetRequest,
) {
    let replica_key = replica.replica;
    debug!(%replica_key,leo=replica.leo,hw=replica.hw);
    let follower_update = FollowerOffsetUpdate {
        follower_id,
        leo: replica.leo,
        hw: replica.hw,
    };

    if let Some(leader) = ctx.leaders_state().get(&replica_key) {
        if let Err(err) = leader
            .send_message_to_controller(LeaderReplicaControllerCommand::FollowerOffsetUpdate(
                follower_update,
            ))
            .await
        {
            error!(
                "Error sending offset updates to leader: {}, err: {}",
                replica_key, err
            )
        }
    } else {
        error!("replica leader: {} was not found", replica_key); // this could happen when leader controller is not happen
    }
}
