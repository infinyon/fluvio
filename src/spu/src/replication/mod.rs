pub(crate) mod follower;
pub(crate) mod leader;

#[cfg(test)]
#[cfg(target_os = "linux")]
mod replica_test {

    use std::path::{PathBuf};
    use std::time::Duration;
    use std::env::temp_dir;

    use fluvio_storage::FileReplica;
    use tracing::debug;
    use derive_builder::Builder;
    use once_cell::sync::Lazy;

    use fluvio_future::{test_async};
    use fluvio_future::timer::sleep;
    use flv_util::fixture::ensure_clean_dir;
    use fluvio_types::SpuId;
    use fluvio_controlplane_metadata::partition::{Replica};
    use fluvio_controlplane_metadata::spu::{SpuSpec};
    use dataplane::fixture::{create_recordset};

    use crate::core::{DefaultSharedGlobalContext, GlobalContext};
    use crate::config::SpuConfig;
    use crate::services::create_internal_server;

    use super::{follower::FollowerReplicaState, leader::LeaderReplicaState};

    const TOPIC: &str = "test";
    const HOST: &str = "127.0.0.1";

    const MAX_BYTES: u32 = 100000;
    const MAX_WAIT_LEADER: u64 = 100;
    const MAX_WAIT_FOLLOWER: u64 = 100;
    const WAIT_TERMINATE: u64 = 1000;

    const LEADER: SpuId = 5001;
    const FOLLOWER1: SpuId = 5002;
    const FOLLOWER2: SpuId = 5003;

    static MAX_WAIT_REPLICATION: Lazy<u64> = Lazy::new(|| {
        use std::env;
        if env::var("CI").is_ok() {
            5000
        } else {
            1000
        }
    });

    #[derive(Builder, Debug)]
    struct TestConfig {
        #[builder(setter(into), default = "5001")]
        base_id: SpuId,
        #[builder(setter(into), default = "1")]
        in_sync_replica: u16,
        #[builder(setter(into), default = "0")]
        followers: u16,
        #[builder(setter(into), default = "temp_dir()")]
        base_dir: PathBuf,
        #[builder(setter(into), default = "9000")]
        base_port: u16,
    }

    impl TestConfig {
        // isolate builder
        pub fn builder() -> TestConfigBuilder {
            TestConfigBuilder::default()
        }

        pub fn leader_config(&self) -> SpuConfig {
            let mut config = SpuConfig::default();
            config.log.base_dir = self.base_dir.clone();
            config.id = self.base_id;
            config.private_endpoint = format!("{}:{}", HOST, self.base_port);
            config
        }

        pub fn follower_config(&self, follower_index: u16) -> SpuConfig {
            assert!(follower_index < self.followers);
            let mut config = SpuConfig::default();
            config.log.base_dir = self.base_dir.clone();
            config.replication.min_in_sync_replicas = self.in_sync_replica;
            config.id = self.follower_id(follower_index);
            config
        }

        fn leader_port(&self) -> u16 {
            self.base_port
        }

        fn follower_port(&self, follower_index: u16) -> u16 {
            assert!(follower_index < self.followers);
            self.base_port + 1 + follower_index
        }

        fn follower_id(&self, follower_index: u16) -> SpuId {
            assert!(follower_index < self.followers);
            self.base_id + 1 + follower_index as SpuId
        }

        fn spu_specs(&self) -> Vec<SpuSpec> {
            let mut specs = vec![SpuSpec::new_private_addr(
                self.base_id,
                self.leader_port(),
                HOST.to_owned(),
            )];

            for i in 0..self.followers {
                specs.push(SpuSpec::new_private_addr(
                    self.follower_id(i),
                    self.follower_port(i),
                    HOST.to_owned(),
                ));
            }
            specs
        }

        fn replica(&self) -> Replica {
            let mut followers = vec![LEADER];
            for i in 0..self.followers {
                followers.push(self.follower_id(i));
            }
            Replica::new((TOPIC, 0), self.base_id, followers)
        }

        pub fn leader_addr(&self) -> String {
            format!("{}:{}", HOST, self.base_port)
        }

        pub async fn leader_replica(
            &self,
        ) -> (DefaultSharedGlobalContext, LeaderReplicaState<FileReplica>) {
            let leader_config = self.leader_config();
            let replica = self.replica();

            let gctx = GlobalContext::new_shared_context(leader_config);
            gctx.spu_localstore().sync_all(self.spu_specs());
            gctx.sync_follower_update().await;

            let leader_replica = gctx
                .leaders_state()
                .add_leader_replica(
                    gctx.clone(),
                    replica.clone(),
                    MAX_BYTES,
                    gctx.status_update_owned(),
                )
                .await
                .expect("leader");

            (gctx, leader_replica)
        }

        pub async fn follower_replica(
            &self,
            follower_index: u16,
        ) -> (
            DefaultSharedGlobalContext,
            FollowerReplicaState<FileReplica>,
        ) {
            let follower_config = self.follower_config(follower_index);
            //debug!(?follower_config);

            let replica = self.replica();
            let gctx = GlobalContext::new_shared_context(follower_config);
            gctx.spu_localstore().sync_all(self.spu_specs());
            gctx.followers_state_owned()
                .add_replica(gctx.clone(), replica.clone())
                .await
                .expect("create");

            let follower_replica = gctx
                .followers_state()
                .get(&replica.id)
                .await
                .expect("value");

            (gctx, follower_replica)
        }
    }

    impl TestConfigBuilder {
        fn generate(&mut self, test_dir: &str) -> TestConfig {
            let mut test_config = self.build().unwrap();
            let test_path = test_config.base_dir.join(test_dir);
            ensure_clean_dir(&test_path);
            test_config.base_dir = test_path;
            test_config
        }
    }

    /// Test 2 replica
    /// Replicating with existing records
    ///    
    #[test_async]
    async fn test_just_leader() -> Result<(), ()> {
        let builder = TestConfig::builder()
            .base_port(13000 as u16)
            .generate("just_leader");

        let (leader_gctx, leader_replica) = builder.leader_replica().await;

        assert_eq!(leader_replica.leo(), 0);
        assert_eq!(leader_replica.hw(), 0);

        let status = leader_gctx.status_update().remove_all().await;
        assert!(!status.is_empty());
        let lrs = &status[0];
        assert_eq!(lrs.id, (TOPIC, 0).into());
        assert_eq!(lrs.leader.spu, LEADER);
        assert_eq!(lrs.leader.hw, 0);
        assert_eq!(lrs.leader.leo, 0);

        // write records
        leader_replica
            .write_record_set(&mut create_recordset(2), leader_gctx.follower_notifier())
            .await
            .expect("write");

        assert_eq!(leader_replica.leo(), 2);
        assert_eq!(leader_replica.hw(), 2);

        let status = leader_gctx.status_update().remove_all().await;
        assert!(!status.is_empty());
        let lrs = &status[0];
        assert_eq!(lrs.id, (TOPIC, 0).into());
        assert_eq!(lrs.leader.spu, LEADER);
        assert_eq!(lrs.leader.hw, 2);
        assert_eq!(lrs.leader.leo, 2);

        Ok(())
    }

    /// Test 2 replica
    /// Replicating with existing records
    ///    
    #[test_async]
    async fn test_replication2_existing() -> Result<(), ()> {
        let builder = TestConfig::builder()
            .followers(1 as u16)
            .base_port(13010 as u16)
            .generate("replication2_existing");

        let (leader_gctx, leader_replica) = builder.leader_replica().await;

        // write records
        leader_replica
            .write_record_set(&mut create_recordset(2), leader_gctx.follower_notifier())
            .await
            .expect("write");

        assert_eq!(leader_replica.leo(), 2);
        assert_eq!(leader_replica.hw(), 0);
        assert!(!leader_gctx.status_update().remove_all().await.is_empty());

        let spu_server = create_internal_server(builder.leader_addr(), leader_gctx.clone()).run();

        // give leader controller time to startup
        sleep(Duration::from_millis(MAX_WAIT_LEADER)).await;

        debug!("starting follower replica controller");
        let (_, follower_replica) = builder.follower_replica(0).await;

        // at this point, follower replica should be empty since we didn't have time to sync up with leader
        assert_eq!(follower_replica.leo(), 0);
        assert_eq!(follower_replica.hw(), 0);

        // wait until follower sync up with leader
        sleep(Duration::from_millis(*MAX_WAIT_REPLICATION)).await;

        debug!("done waiting. checking result");

        // all records has been fully replicated
        assert_eq!(follower_replica.leo(), 2);

        // hw has been replicated
        assert_eq!(follower_replica.hw(), 2);
        assert_eq!(leader_replica.hw(), 2);

        let status = leader_gctx.status_update().remove_all().await;
        debug!(?status);
        assert!(!status.is_empty());
        let lrs = &status[0];
        assert_eq!(lrs.id, (TOPIC, 0).into());
        assert_eq!(lrs.leader.spu, LEADER);
        assert_eq!(lrs.leader.hw, 2);
        assert_eq!(lrs.leader.leo, 2);
        let f_status = &lrs.replicas[0];
        assert_eq!(f_status.spu, FOLLOWER1);
        assert_eq!(f_status.hw, 2);
        assert_eq!(f_status.leo, 2);

        sleep(Duration::from_millis(WAIT_TERMINATE)).await;

        spu_server.notify();

        Ok(())
    }

    /// Test 2 replica
    /// Replicating new records
    ///    
    //#[test_async]
    #[allow(unused)]
    async fn test_replication2_new_records() -> Result<(), ()> {
        let builder = TestConfig::builder()
            .followers(1 as u16)
            .base_port(13020 as u16)
            .generate("replication2_new");

        let (leader_gctx, leader_replica) = builder.leader_replica().await;
        assert_eq!(leader_replica.leo(), 0);
        assert_eq!(leader_replica.hw(), 0);

        let follower_info = leader_replica.followers_info().await;
        assert_eq!(follower_info.get(&5002).unwrap().leo, -1);

        let spu_server = create_internal_server(builder.leader_addr(), leader_gctx.clone()).run();

        // give leader controller time to startup
        sleep(Duration::from_millis(MAX_WAIT_LEADER)).await;

        let (follower_ctx, follower_replica) = builder.follower_replica(0).await;

        // at this point, follower replica should be empty since we didn't have time to sync up with leader
        assert_eq!(follower_replica.leo(), 0);
        assert_eq!(follower_replica.hw(), 0);

        sleep(Duration::from_millis(MAX_WAIT_FOLLOWER)).await;
        // leader should have actual follower info not just init
        let follower_info = leader_replica.followers_info().await;
        assert_eq!(follower_info.get(&5002).unwrap().leo, 0);

        // write records
        leader_replica
            .write_record_set(&mut create_recordset(2), leader_gctx.follower_notifier())
            .await
            .expect("write");

        assert_eq!(leader_replica.leo(), 2);
        assert_eq!(leader_replica.hw(), 0);

        // wait until follower sync up with leader
        sleep(Duration::from_millis(*MAX_WAIT_REPLICATION)).await;

        debug!("done waiting. checking result");

        // all records has been fully replicated
        assert_eq!(follower_replica.leo(), 2);

        // hw has been replicated
        assert_eq!(follower_replica.hw(), 2);
        assert_eq!(leader_replica.hw(), 2);

        let status = leader_gctx.status_update().remove_all().await;
        debug!(?status);
        assert!(!status.is_empty());
        let lrs = &status[0];
        assert_eq!(lrs.id, (TOPIC, 0).into());
        assert_eq!(lrs.leader.spu, LEADER);
        assert_eq!(lrs.leader.hw, 2);
        assert_eq!(lrs.leader.leo, 2);
        let f_status = &lrs.replicas[0];
        assert_eq!(f_status.spu, FOLLOWER1);
        assert_eq!(f_status.hw, 2);
        assert_eq!(f_status.leo, 2);

        sleep(Duration::from_millis(WAIT_TERMINATE)).await;

        spu_server.notify();

        Ok(())
    }

    /// test with 3 SPU
    #[test_async]
    async fn test_replication3_existing() -> Result<(), ()> {
        let builder = TestConfig::builder()
            .followers(2 as u16)
            .base_port(13030 as u16)
            .generate("replication3_existing");

        let (leader_gctx, leader_replica) = builder.leader_replica().await;

        // write records
        leader_replica
            .write_record_set(&mut create_recordset(2), leader_gctx.follower_notifier())
            .await
            .expect("write");

        assert_eq!(leader_replica.leo(), 2);
        assert_eq!(leader_replica.hw(), 0);

        let spu_server = create_internal_server(builder.leader_addr(), leader_gctx.clone()).run();

        // give leader controller time to startup
        sleep(Duration::from_millis(MAX_WAIT_LEADER)).await;

        let (_, follower_replica1) = builder.follower_replica(0).await;

        // at this point, follower replica should be empty since we didn't have time to sync up with leader
        assert_eq!(follower_replica1.leo(), 0);
        assert_eq!(follower_replica1.hw(), 0);

        let (_, follower_replica2) = builder.follower_replica(1).await;

        // at this point, follower replica should be empty since we didn't have time to sync up with leader
        assert_eq!(follower_replica2.leo(), 0);
        assert_eq!(follower_replica2.hw(), 0);

        //wait until follower sync up with leader
        sleep(Duration::from_millis(*MAX_WAIT_REPLICATION)).await;

        debug!("done waiting for first checking result");

        // all records has been fully replicated
        assert_eq!(follower_replica1.leo(), 2);
        // leader's hw is still 0
        assert_eq!(follower_replica1.hw(), 2);
        assert_eq!(leader_replica.hw(), 2);

        let (_, follower_replica2) = builder.follower_replica(1).await;
        assert_eq!(follower_replica2.leo(), 2);
        assert_eq!(follower_replica2.hw(), 2);

        sleep(Duration::from_millis(WAIT_TERMINATE)).await;

        spu_server.notify();

        Ok(())
    }

    /// Test 2 replica
    /// Replicating new records
    ///    
    #[test_async]
    async fn test_replication3_new_records() -> Result<(), ()> {
        let builder = TestConfig::builder()
            .followers(2 as u16)
            .base_port(13040 as u16)
            .generate("replication3_new");

        let (leader_gctx, leader_replica) = builder.leader_replica().await;
        assert_eq!(leader_replica.leo(), 0);
        assert_eq!(leader_replica.hw(), 0);

        let follower_info = leader_replica.followers_info().await;
        assert_eq!(follower_info.get(&FOLLOWER1).unwrap().leo, -1);
        assert_eq!(follower_info.get(&FOLLOWER2).unwrap().leo, -1);

        let spu_server = create_internal_server(builder.leader_addr(), leader_gctx.clone()).run();

        // give leader controller time to startup
        sleep(Duration::from_millis(MAX_WAIT_LEADER)).await;

        let (_, follower_replica1) = builder.follower_replica(0).await;

        // at this point, follower replica should be empty since we didn't have time to sync up with leader
        assert_eq!(follower_replica1.leo(), 0);
        assert_eq!(follower_replica1.hw(), 0);

        let (_, follower_replica2) = builder.follower_replica(1).await;

        // at this point, follower replica should be empty since we didn't have time to sync up with leader
        assert_eq!(follower_replica2.leo(), 0);
        assert_eq!(follower_replica2.hw(), 0);

        // wait for followers to sync with leader
        sleep(Duration::from_millis(MAX_WAIT_FOLLOWER)).await;

        // leader should now states from follower
        let follower_info = leader_replica.followers_info().await;
        assert_eq!(follower_info.get(&FOLLOWER1).unwrap().leo, 0);
        assert_eq!(follower_info.get(&FOLLOWER2).unwrap().leo, 0);

        // write records
        leader_replica
            .write_record_set(&mut create_recordset(2), leader_gctx.follower_notifier())
            .await
            .expect("write");

        assert_eq!(leader_replica.leo(), 2);
        assert_eq!(leader_replica.hw(), 0);

        // wait until follower sync up with leader
        sleep(Duration::from_millis(*MAX_WAIT_REPLICATION)).await;

        debug!("done waiting. checking result");

        // all records has been fully replicated
        assert_eq!(follower_replica1.leo(), 2);
        assert_eq!(follower_replica1.hw(), 2);
        assert_eq!(leader_replica.hw(), 2);
        assert_eq!(follower_replica2.leo(), 2);
        assert_eq!(follower_replica2.hw(), 2);

        // await while controllers terminate
        sleep(Duration::from_millis(WAIT_TERMINATE)).await;

        spu_server.notify();

        Ok(())
    }
}
