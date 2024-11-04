use std::{sync::Arc, time::Duration};

use fluvio_auth::root::RootAuthorization;
use tracing::debug;

use fluvio_controlplane_metadata::partition::{RemotePartitionConfig, HomePartitionConfig};
use fluvio_future::timer::sleep;
use fluvio_protocol::{fixture::create_raw_recordset, record::ReplicaKey};

use crate::{
    mirroring::test::fixture::{default_home_cluster, default_replica, default_topic},
    services::{auth::SpuAuthGlobalContext, public::create_public_server},
};

use super::fixture::{ReplicaConfig, local_port};

const REMOTE1: &str = "remote1";
const REMOTE2: &str = "remote2";

/// Test mirroring when we write new records when all clusters are up
#[fluvio_future::test(ignore)]
async fn test_mirroring_from_edge_to_home() {
    // find free port for home
    let home_port = local_port();

    let home_builder = ReplicaConfig::builder()
        .remote_clusters(vec![REMOTE1.to_owned(), REMOTE2.to_owned()])
        .generate("mirror_home");
    let home_gctx = home_builder.init_mirror_home().await;
    let home_replica0 = home_gctx
        .leaders_state()
        .get(&ReplicaKey::new(default_topic(), 0u32))
        .await
        .expect("leader");
    assert_eq!(
        home_replica0
            .get_replica()
            .mirror
            .as_ref()
            .expect("mirror")
            .home()
            .expect("home"),
        &HomePartitionConfig {
            remote_cluster: REMOTE1.to_owned(),
            remote_replica: default_replica().to_owned(),
            source: false
        }
    );
    // check if remote cluster is set
    let remote_cluster = home_gctx
        .mirrors_localstore()
        .spec(&REMOTE1.to_owned())
        .expect("remote cluster");
    assert_eq!(remote_cluster.name, REMOTE1);

    debug!(remote_clusters = ?home_gctx.mirrors_localstore(),  "home clusters remotes");
    debug!(replicas = ?home_gctx.leaders_state().replica_configs().await, "home leaders");
    let (mirror_home_replica, source) = home_gctx
        .leaders_state()
        .find_mirror_home_leader(REMOTE1, default_replica())
        .await
        .expect("mirror home");
    assert!(!source);
    assert_eq!(mirror_home_replica.id(), &(default_topic(), 0).into());
    assert_eq!(home_replica0.leo(), 0);

    // check 2nd home replica
    let home_replica1 = home_gctx
        .leaders_state()
        .get(&ReplicaKey::new(default_topic(), 1u32))
        .await
        .expect("2nd targert");

    // start home server
    debug!("starting home server");

    let auth_global_ctx =
        SpuAuthGlobalContext::new(home_gctx.clone(), Arc::new(RootAuthorization::new()));
    let _remote_end = create_public_server(home_port.to_owned(), auth_global_ctx.clone()).run();

    // sleep 1 seconds
    debug!("waiting for home public server to up");
    sleep(Duration::from_secs(1)).await;
    debug!("done waiting");

    // start 1st remote
    let sourcd_builder_1 = ReplicaConfig::builder()
        .home_port(home_port.clone())
        .remote_cluster(REMOTE1)
        .generate("mirror_remote");

    let (remote_ctx1, remote_replica_1) = sourcd_builder_1.init_mirror_remote().await;
    let remote_mirror1 = remote_replica_1
        .get_replica()
        .mirror
        .as_ref()
        .expect("mirror");
    assert_eq!(
        remote_mirror1.remote().expect("remote"),
        &RemotePartitionConfig {
            home_spu_key: default_replica().to_owned(),
            home_cluster: default_home_cluster().to_owned(),
            home_spu_id: 5001,
            home_spu_endpoint: home_port.clone(),
            target: false
        }
    );

    // sleep 1 seconds
    debug!("waiting for mirror remote controller to startup");
    sleep(Duration::from_secs(1)).await;
    debug!("done waiting");

    remote_replica_1
        .write_record_set(
            &mut create_raw_recordset(2),
            remote_ctx1.follower_notifier(),
        )
        .await
        .expect("write");

    assert_eq!(remote_replica_1.leo(), 2);

    // wait to replicate
    debug!("waiting for mirroring");
    sleep(Duration::from_secs(5)).await;
    debug!("done waiting");

    // home should have recods
    assert_eq!(home_replica0.leo(), 2);

    // start 2nd remote
    let sourcd_builder2 = ReplicaConfig::builder()
        .home_port(home_port.clone())
        .remote_cluster(REMOTE2)
        .generate("mirror_remote");

    let (_remote_ctx2, remote_replica2) = sourcd_builder2.init_mirror_remote().await;
    let remote_mirror2 = remote_replica2
        .get_replica()
        .mirror
        .as_ref()
        .expect("mirror");
    assert_eq!(
        remote_mirror2.remote().expect("remote"),
        &RemotePartitionConfig {
            home_spu_key: default_replica().to_owned(),
            home_cluster: default_home_cluster().to_owned(),
            home_spu_id: 5001,
            home_spu_endpoint: home_port.clone(),
            ..Default::default()
        }
    );

    // sleep 1 seconds
    debug!("waiting for mirror remote controller 2nd to start up");
    sleep(Duration::from_secs(1)).await;
    debug!("done waiting");

    remote_replica2
        .write_record_set(
            &mut create_raw_recordset(2),
            remote_ctx1.follower_notifier(),
        )
        .await
        .expect("write");

    assert_eq!(remote_replica2.leo(), 2);

    debug!("waiting for mirroring");
    sleep(Duration::from_secs(5)).await;
    debug!("done waiting");
    // home should have recods
    assert_eq!(home_replica1.leo(), 2);
}

/// Test mirroring from home to edge
#[fluvio_future::test(ignore)]
async fn test_mirror_home_to_edge() {
    // find free port for home
    let home_port = local_port();

    let home_builder = ReplicaConfig::builder()
        .remote_clusters(vec![REMOTE1.to_owned(), REMOTE2.to_owned()])
        .home_to_remote(true)
        .generate("mirror_home");
    let home_gctx = home_builder.init_mirror_home().await;
    let home_replica0 = home_gctx
        .leaders_state()
        .get(&ReplicaKey::new(default_topic(), 0u32))
        .await
        .expect("leader");
    assert_eq!(
        home_replica0
            .get_replica()
            .mirror
            .as_ref()
            .expect("mirror")
            .home()
            .expect("home"),
        &HomePartitionConfig {
            remote_cluster: REMOTE1.to_owned(),
            remote_replica: default_replica().to_owned(),
            source: true
        }
    );
    // check if remote cluster is set
    let remote_cluster = home_gctx
        .mirrors_localstore()
        .spec(&REMOTE1.to_owned())
        .expect("remote cluster");
    assert_eq!(remote_cluster.name, REMOTE1);

    debug!(remote_clusters = ?home_gctx.mirrors_localstore(),  "home clusters remotes");
    debug!(replicas = ?home_gctx.leaders_state().replica_configs().await, "home leaders");
    let (mirror_home_replica, source) = home_gctx
        .leaders_state()
        .find_mirror_home_leader(REMOTE1, default_replica())
        .await
        .expect("mirror home");
    assert!(source);
    assert_eq!(mirror_home_replica.id(), &(default_topic(), 0).into());
    assert_eq!(home_replica0.leo(), 0);

    // check 2nd home replica

    let home_replica1 = home_gctx
        .leaders_state()
        .get(&ReplicaKey::new(default_topic(), 1u32))
        .await
        .expect("2nd targert");

    // start home server
    debug!("starting home server");

    let auth_global_ctx =
        SpuAuthGlobalContext::new(home_gctx.clone(), Arc::new(RootAuthorization::new()));
    let _remote_end = create_public_server(home_port.to_owned(), auth_global_ctx.clone()).run();

    // sleep 1 seconds
    debug!("waiting for home public server to up");
    sleep(Duration::from_secs(1)).await;
    debug!("done waiting");

    // start 1st remote
    let sourcd_builder_1 = ReplicaConfig::builder()
        .home_port(home_port.clone())
        .remote_cluster(REMOTE1)
        .home_to_remote(true)
        .generate("mirror_remote");

    let (_remote_ctx1, remote_replica_1) = sourcd_builder_1.init_mirror_remote().await;
    let remote_mirror1 = remote_replica_1
        .get_replica()
        .mirror
        .as_ref()
        .expect("mirror");
    assert_eq!(
        remote_mirror1.remote().expect("remote"),
        &RemotePartitionConfig {
            home_spu_key: default_replica().to_owned(),
            home_cluster: default_home_cluster().to_owned(),
            home_spu_id: 5001,
            home_spu_endpoint: home_port.clone(),
            target: true
        }
    );

    // sleep 1 seconds
    debug!("waiting for mirror remote controller to startup");
    sleep(Duration::from_secs(1)).await;
    debug!("done waiting");

    // write records from home
    home_replica0
        .write_record_set(&mut create_raw_recordset(2), home_gctx.follower_notifier())
        .await
        .expect("write");

    assert_eq!(home_replica0.leo(), 2);

    // wait to replicate
    debug!("waiting for mirroring");
    sleep(Duration::from_secs(2)).await;
    debug!("done waiting");

    // home should have recods
    assert_eq!(remote_replica_1.leo(), 2);

    // start 2nd remote
    let sourcd_builder2 = ReplicaConfig::builder()
        .home_port(home_port.clone())
        .remote_cluster(REMOTE2)
        .home_to_remote(true)
        .generate("mirror_remote");

    let (_remote_ctx2, remote_replica2) = sourcd_builder2.init_mirror_remote().await;
    let remote_mirror2 = remote_replica2
        .get_replica()
        .mirror
        .as_ref()
        .expect("mirror");
    assert_eq!(
        remote_mirror2.remote().expect("remote"),
        &RemotePartitionConfig {
            home_spu_key: default_replica().to_owned(),
            home_cluster: default_home_cluster().to_owned(),
            home_spu_id: 5001,
            home_spu_endpoint: home_port.clone(),
            target: true
        }
    );

    // sleep 1 seconds
    debug!("waiting for mirror remote controller 2nd to start up");
    sleep(Duration::from_secs(1)).await;
    debug!("done waiting");

    // write records from home
    home_replica1
        .write_record_set(&mut create_raw_recordset(2), home_gctx.follower_notifier())
        .await
        .expect("write");

    assert_eq!(home_replica1.leo(), 2);

    debug!("waiting for mirroring");
    sleep(Duration::from_secs(5)).await;
    debug!("done waiting");
    // home should have recods
    assert_eq!(home_replica1.leo(), 2);
}
