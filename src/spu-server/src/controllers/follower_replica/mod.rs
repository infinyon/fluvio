mod state;
mod follower_controller;
mod api_key;
mod peer_api;
mod sync;

pub(crate) use self::follower_controller::ReplicaFollowerController;
pub use self::state::FollowersState;
pub use self::state::FollowerReplicaState;
pub use self::state::SharedFollowersState;
pub use self::api_key::KfFollowerPeerApiEnum;
pub use self::peer_api::FollowerPeerRequest;
pub use self::sync::PeerFileTopicResponse;
pub use self::sync::PeerFilePartitionResponse;
pub use self::sync::DefaultSyncRequest;
pub use self::sync::FileSyncRequest;

use flv_metadata::partition::Replica;

#[derive(Debug)]
pub enum FollowerReplicaControllerCommand {
    AddReplica(Replica),
    UpdateReplica(Replica),
}
