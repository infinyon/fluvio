pub(crate) mod follower;
pub(crate) mod leader;

#[cfg(test)]
#[cfg(target_os = "linux")]
mod replica_test {

    use std::path::PathBuf;
    use std::time::Duration;
    use std::env::temp_dir;

    use tracing::debug;

    use fluvio_future::{test_async};
    use fluvio_future::timer::sleep;
    use flv_util::fixture::ensure_clean_dir;
    use fluvio_types::SpuId;
    use fluvio_controlplane_metadata::partition::{Replica};
    use fluvio_controlplane_metadata::spu::{SpuSpec};
    use dataplane::fixture::{create_recordset};

    use crate::core::GlobalContext;
    use crate::config::SpuConfig;
    use crate::services::create_internal_server;
    use crate::control_plane::{ScSinkMessageChannel};

    const LEADER: SpuId = 5001;
    const FOLLOWER: SpuId = 5002;
    const LEADER_PORT: u16 = 13000;
    const FOLLOWER_PORT: u16 = 13001;
    const TOPIC: &str = "test";
    const HOST: &str = "127.0.0.1";
    fn leader_addr() -> String {
        format!("{}:{}", HOST, LEADER_PORT)
    }

    const MAX_BYTES: u32 = 100000;
    fn spu_specs() -> Vec<SpuSpec> {
        vec![
            SpuSpec::new_private_addr(LEADER, LEADER_PORT, HOST.to_owned()),
            SpuSpec::new_private_addr(FOLLOWER, FOLLOWER_PORT, HOST.to_owned()),
        ]
    }

    
    
    /// Test 2 replica
    /// Replicating with existing records
    ///    
    #[test_async]
    async fn test_initial_replication() -> Result<(), ()> {
        let test_path = temp_dir().join("replication_test");
        ensure_clean_dir(&test_path);

        let replica = Replica::new((TOPIC, 0), LEADER, vec![FOLLOWER]);

        let mut leader_config = SpuConfig::default();
        leader_config.log.base_dir = test_path.clone();
        leader_config.replication.min_in_sync_replicas = 2;
        leader_config.id = LEADER;

        leader_config.private_endpoint = leader_addr();
        let leader_gctx = GlobalContext::new_shared_context(leader_config);
        leader_gctx.spu_localstore().sync_all(spu_specs());

        let leader_replica = leader_gctx
            .leaders_state()
            .add_leader_replica(
                leader_gctx.clone(),
                replica.clone(),
                MAX_BYTES,
                ScSinkMessageChannel::shared(),
            )
            .await
            .expect("leader");

        // write records
        leader_replica
            .write_record_set(&mut create_recordset(2))
            .await
            .expect("write");

        assert_eq!(leader_replica.leo(), 2);
        assert_eq!(leader_replica.hw(), 0);

        let spu_server = create_internal_server(leader_addr(), leader_gctx.clone()).run();

        // sleep little bit until we spin up follower
        sleep(Duration::from_millis(100)).await;

        let mut follower_config = SpuConfig::default();
        follower_config.log.base_dir = test_path;
        follower_config.id = FOLLOWER;
        let follower_gctx = GlobalContext::new_shared_context(follower_config);
        follower_gctx.spu_localstore().sync_all(spu_specs());
        follower_gctx
            .followers_state_owned()
            .add_replica(follower_gctx.clone(), replica.clone())
            .await
            .expect("create");

        let follower_replica = follower_gctx
            .followers_state()
            .get(&replica.id)
            .expect("follower");

        // at this point, follower replica should be empty since we didn't have time to sync up with leader
        assert_eq!(follower_replica.leo(), 0);
        assert_eq!(follower_replica.hw(), 0);

        // wait until follower sync up with leader
        sleep(Duration::from_millis(1000)).await;

        debug!("done waiting. checking result");

        // all records has been fully replicated
        assert_eq!(follower_replica.leo(), 2);

        // hw has been replicated
        assert_eq!(follower_replica.hw(), 2);
        assert_eq!(leader_replica.hw(), 2);

        spu_server.notify();

        Ok(())
    }


    /// Test 2 replica
    /// Replicating new records
    ///    
    #[test_async]
    async fn test_replication2_new_records() -> Result<(), ()> {
        let test_path = "/tmp/replication_test";
        ensure_clean_dir(test_path);

        let replica = Replica::new((TOPIC, 0), LEADER, vec![FOLLOWER]);

        let mut leader_config = SpuConfig::default();
        leader_config.log.base_dir = PathBuf::from(test_path);
        leader_config.replication.min_in_sync_replicas = 2;
        leader_config.id = LEADER;

        leader_config.private_endpoint = leader_addr();
        let leader_gctx = GlobalContext::new_shared_context(leader_config);
        leader_gctx.spu_localstore().sync_all(spu_specs());

        let leader_replica = leader_gctx
            .leaders_state()
            .add_leader_replica(
                leader_gctx.clone(),
                replica.clone(),
                MAX_BYTES,
                ScSinkMessageChannel::shared(),
            )
            .await
            .expect("leader");

        

        let spu_server = create_internal_server(leader_addr(), leader_gctx.clone()).run();

        // sleep little bit until we spin up follower
        sleep(Duration::from_millis(100)).await;

        let mut follower_config = SpuConfig::default();
        follower_config.log.base_dir = PathBuf::from(test_path);
        follower_config.id = FOLLOWER;
        let follower_gctx = GlobalContext::new_shared_context(follower_config);
        follower_gctx.spu_localstore().sync_all(spu_specs());
        follower_gctx
            .followers_state_owned()
            .add_replica(follower_gctx.clone(), replica.clone())
            .await
            .expect("create");

        let follower_replica = follower_gctx
            .followers_state()
            .get(&replica.id)
            .expect("follower");

        // at this point, follower replica should be empty since we didn't have time to sync up with leader
        assert_eq!(follower_replica.leo(), 0);
        assert_eq!(follower_replica.hw(), 0);

        // write records
        leader_replica
            .write_record_set(&mut create_recordset(2))
            .await
            .expect("write");

        assert_eq!(leader_replica.leo(), 2);
        assert_eq!(leader_replica.hw(), 0);

        // wait until follower sync up with leader
        sleep(Duration::from_millis(1000)).await;

        debug!("done waiting. checking result");

        // all records has been fully replicated
        assert_eq!(follower_replica.leo(), 2);

        // hw has been replicated
        assert_eq!(follower_replica.hw(), 2);
        assert_eq!(leader_replica.hw(), 2);

        spu_server.notify();

        Ok(())
    }
}
