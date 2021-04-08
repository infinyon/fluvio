pub(crate) mod follower_replica;
pub(crate) mod leader_replica;
pub(crate) mod sc;

#[cfg(test)]
mod replica_test {

    use std::path::PathBuf;

    use fluvio_future::{test_async};
    //use fluvio_future::timer::sleep;
    use flv_util::fixture::ensure_clean_dir;
    use fluvio_types::SpuId;
    use fluvio_controlplane_metadata::partition::{Replica};
    use dataplane::record::RecordSet;
    use dataplane::fixture::{create_batch};

    use crate::core::GlobalContext;
    use crate::config::SpuConfig;
    use super::sc::{ScSinkMessageChannel};

    const LEADER: SpuId = 5001;
    const FOLLOWER: SpuId = 5002;
    const TOPIC: &str = "test";
    const MAX_BYTES: u32 = 100000;

    /// test a single leader and follower
    #[test_async]
    async fn test_initial_replication() -> Result<(), ()> {
        let test_path = "/tmp/replication_test";
        ensure_clean_dir(test_path);

        //let addr = "127.0.0.1:12000";

        let mut leader_config = SpuConfig::default();
        leader_config.log.base_dir = PathBuf::from(test_path);
        leader_config.id = LEADER;
        let leader_gctx = GlobalContext::new_shared_context(leader_config);
        let replica = Replica::new((TOPIC, 0).into(), LEADER, vec![FOLLOWER]);
        let leader_replica = leader_gctx
            .leaders_state()
            .add_leader_replica(
                leader_gctx.clone(),
                replica,
                MAX_BYTES,
                ScSinkMessageChannel::shared(),
            )
            .await
            .expect("leader");

        let mut follower_config = SpuConfig::default();
        follower_config.log.base_dir = PathBuf::from(test_path);
        follower_config.id = FOLLOWER;
        let follower_gctx = GlobalContext::new_shared_context(follower_config);
        let replica = Replica::new((TOPIC, 0).into(), LEADER, vec![FOLLOWER]);
        let _follower_replica = follower_gctx
            .leaders_state()
            .add_leader_replica(
                leader_gctx.clone(),
                replica,
                MAX_BYTES,
                ScSinkMessageChannel::shared(),
            )
            .await
            .expect("follower");

        let mut records = RecordSet::default().add(create_batch());
        leader_replica
            .write_record_set(&mut records)
            .await
            .expect("write");

        Ok(())
    }
}
