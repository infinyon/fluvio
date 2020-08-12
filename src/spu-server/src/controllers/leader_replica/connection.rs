use tracing::trace;
use tracing::error;
use tracing::debug;
use tracing::warn;
use kf_socket::KfSocketError;
use kf_socket::KfStream;
use kf_socket::KfSocket;
use kf_service::api_loop;
use flv_types::SpuId;

use crate::core::DefaultSharedGlobalContext;

use super::LeaderReplicaControllerCommand;
use super::FollowerOffsetUpdate;
use super::KfLeaderPeerApiEnum;
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
        socket: KfSocket,
    ) -> Result<(), KfSocketError> {
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

    async fn main_loop(&self, mut stream: KfStream) -> Result<(), KfSocketError> {
        trace!(
            "starting connection handling from follower: {} for leader: {}",
            self.follower_id,
            self.ctx.local_spu_id()
        );

        let mut api_stream = stream.api_stream::<LeaderPeerRequest, KfLeaderPeerApiEnum>();

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
    let follower_update = FollowerOffsetUpdate {
        follower_id: follower_id,
        leo: replica.leo,
        hw: replica.hw,
    };

    match ctx
        .leaders_state()
        .send_message(
            &replica_key,
            LeaderReplicaControllerCommand::FollowerOffsetUpdate(follower_update),
        )
        .await
    {
        Ok(success_stat) => {
            if success_stat {
                trace!("send offset data to: replica leader: {}", replica_key);
            } else {
                warn!("replica leader: {} was not found", replica_key); // this could happen when leader controller is not happen
            }
        }
        Err(err) => error!(
            "Error writing message to replica {}, err: {}",
            replica_key, err
        ),
    }
}
