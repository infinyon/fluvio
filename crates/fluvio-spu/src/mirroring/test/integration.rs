use std::time::Duration;

use tracing::debug;

use fluvio_controlplane_metadata::partition::{SourcePartitionConfig, TargetPartitionConfig};
use fluvio_future::timer::sleep;
use fluvio_protocol::{fixture::create_raw_recordset, record::ReplicaKey};

use crate::services::public::create_public_server;

use super::fixture::{ReplicaConfig, local_port};

/// Test mirroring when we write new records when all clusters are up
#[fluvio_future::test(ignore)]
async fn test_mirroring_new_records() {
    // find free port for target
    let target_port = local_port();

    let target_builder = ReplicaConfig::builder()
        .remote_clusters(vec!["edge1".to_owned(), "edge2".to_owned()])
        .generate("mirror_target");
    let target_gctx = target_builder.init_mirror_target().await;
    let target_replica0 = target_gctx
        .leaders_state()
        .get(&ReplicaKey::new("temp", 0u32))
        .await
        .expect("leader");
    assert_eq!(
        target_replica0
            .get_replica()
            .mirror
            .as_ref()
            .expect("mirror")
            .target()
            .expect("target"),
        &TargetPartitionConfig {
            remote_cluster: "edge1".to_owned(),
            source_replica: "temp-0".to_owned(),
        }
    );
    // check if remote cluster is set
    let remote_cluster = target_gctx
        .remote_cluster_localstore()
        .spec(&"edge1".to_string())
        .expect("remote cluster");
    assert_eq!(remote_cluster.name, "edge1");

    debug!(remote_clusters = ?target_gctx.remote_cluster_localstore(),  "target clusters remotes");
    debug!(replicas = ?target_gctx.leaders_state().replica_configs().await, "target leaders");
    let mirror_target_replica = target_gctx
        .leaders_state()
        .find_mirror_target_leader("edge1", "temp-0")
        .await
        .expect("mirror target");
    assert_eq!(mirror_target_replica.id(), &("temp", 0).into());
    assert_eq!(target_replica0.leo(), 0);

    // check 2nd target replica
    let target_replica1 = target_gctx
        .leaders_state()
        .get(&ReplicaKey::new("temp", 1u32))
        .await
        .expect("2nd targert");

    // start target server
    debug!("starting target server");
    let _source_end = create_public_server(target_port.clone(), target_gctx.clone()).run();

    // sleep 1 seconds
    debug!("waiting for target public server to up");
    sleep(Duration::from_secs(1)).await;
    debug!("done waiting");

    // create mirror source, this will automatically create controller
    let sourcd_builder_1 = ReplicaConfig::builder()
        .upstream_port(target_port.clone())
        .upstream_cluster("edge1".to_owned())
        .generate("mirror_source");

    let (source_ctx1, source_replica_1) = sourcd_builder_1.init_mirror_source().await;
    let source_mirror1 = source_replica_1
        .get_replica()
        .mirror
        .as_ref()
        .expect("mirror");
    assert_eq!(
        source_mirror1.source().expect("source"),
        &SourcePartitionConfig {
            upstream_cluster: "edge1".to_owned(),
            target_spu: 5001
        }
    );

    // sleep 1 seconds
    debug!("waiting for mirror source controller to startup");
    sleep(Duration::from_secs(1)).await;
    debug!("done waiting");

    source_replica_1
        .write_record_set(
            &mut create_raw_recordset(2),
            source_ctx1.follower_notifier(),
        )
        .await
        .expect("write");

    assert_eq!(source_replica_1.leo(), 2);

    // wait to replicate
    debug!("waiting for mirroring");
    sleep(Duration::from_secs(3)).await;
    debug!("done waiting");

    // target should have recods
    // assert!(mirror_source1.get_metrics().get_loop_count() > 0);
    assert_eq!(target_replica0.leo(), 2);

    // start 2nd source
    let sourcd_builder2 = ReplicaConfig::builder()
        .upstream_port(target_port.clone())
        .upstream_cluster("edge2".to_owned())
        .generate("mirror_source");

    let (_source_ctx2, source_replica2) = sourcd_builder2.init_mirror_source().await;
    let source_mirror2 = source_replica2
        .get_replica()
        .mirror
        .as_ref()
        .expect("mirror");
    assert_eq!(
        source_mirror2.source().expect("source"),
        &SourcePartitionConfig {
            upstream_cluster: "edge2".to_owned(),
            target_spu: 5001
        }
    );

    // sleep 1 seconds
    debug!("waiting for mirror source controller 2nd to start up");
    sleep(Duration::from_secs(1)).await;
    debug!("done waiting");

    source_replica2
        .write_record_set(
            &mut create_raw_recordset(2),
            source_ctx1.follower_notifier(),
        )
        .await
        .expect("write");

    assert_eq!(source_replica2.leo(), 2);

    debug!("waiting for mirroring");
    sleep(Duration::from_secs(3)).await;
    debug!("done waiting");
    // target should have recods
    // assert!(mirror_source1.get_metrics().get_loop_count() > 0);
    assert_eq!(target_replica1.leo(), 2);

    // start target
}
