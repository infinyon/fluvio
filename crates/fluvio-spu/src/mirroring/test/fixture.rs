use std::env::temp_dir;
use std::path::PathBuf;

use fluvio_controlplane::remote_cluster::RemoteClusterSpec;
use fluvio_controlplane::replica::Replica;
use fluvio_controlplane::spu_api::update_remote_cluster::RemoteCluster;
use fluvio_controlplane::spu_api::update_upstream_cluster::UpstreamCluster;
use fluvio_controlplane::upstream_cluster::{UpstreamClusterSpec, UpstreamTarget};
use fluvio_controlplane_metadata::partition::{
    PartitionMirrorConfig, TargetPartitionConfig, SourcePartitionConfig,
};
use fluvio_controlplane_metadata::spu::{IngressPort, SpuSpec, IngressAddr, Endpoint};
use fluvio_protocol::fixture::create_raw_recordset;
use fluvio_protocol::record::ReplicaKey;
use fluvio_storage::FileReplica;

use derive_builder::Builder;
use fluvio_types::{SpuId, PartitionId};
use flv_util::fixture::ensure_clean_dir;

use crate::config::SpuConfig;
use crate::core::{DefaultSharedGlobalContext, GlobalContext};
use crate::replication::leader::LeaderReplicaState;

pub(crate) fn default_topic() -> String {
    "temp".to_owned()
}

pub(crate) fn default_host() -> String {
    "127.0.0.1".to_owned()
}

// find unused port in local host
pub(crate) fn local_port() -> String {
    let port = portpicker::pick_unused_port().expect("No free ports left");
    format!("127.0.0.1:{port}")
}

fn default_upstream_port() -> String {
    "localhost:30000".to_owned()
}

fn default_upstream_cluster() -> String {
    "my-upstream".to_owned()
}

pub(crate) fn default_source_topic() -> String {
    default_topic()
}

#[derive(Builder, Debug)]
pub(crate) struct ReplicaConfig {
    #[builder(default = "temp_dir()")]
    base_dir: PathBuf,
    #[builder(default = "9000")]
    base_port: u16,
    #[builder(default = "0")]
    followers: u16,
    #[builder(default = "default_topic()")]
    topic: String,
    #[builder(default = "5001")]
    base_spu_id: SpuId,
    #[builder(default = "default_host()")]
    host: String,
    #[builder(default = "default_upstream_port()")]
    upstream_port: String,
    #[builder(default = "default_upstream_cluster()")]
    upstream_cluster: String,
    /// if set then this is mirror target and we create multiple target partitions
    #[builder(default)]
    remote_clusters: Vec<String>,
    #[builder(default = "default_source_topic()")]
    source_topic: String,
}

impl ReplicaConfig {
    pub fn builder() -> ReplicaConfigBuilder {
        ReplicaConfigBuilder::default()
    }

    fn follower_id(&self, follower_index: u16) -> SpuId {
        assert!(follower_index < self.followers);
        self.base_spu_id + 1 + follower_index as SpuId
    }

    #[inline]
    fn leader_id(&self) -> SpuId {
        self.base_spu_id
    }

    #[inline]
    fn leader_private_port(&self) -> u16 {
        self.base_port
    }

    fn leader_public_port(&self) -> u16 {
        self.base_port + 1
    }

    // follower port is always private
    fn follower_port(&self, follower_index: u16) -> u16 {
        assert!(follower_index < self.followers);
        self.base_port + 2 + follower_index
    }

    fn replica_with_partition(&self, partition: PartitionId) -> Replica {
        let leader = self.leader_id();
        let mut followers = vec![self.leader_id()];
        for i in 0..self.followers {
            followers.push(self.follower_id(i));
        }
        Replica::new((self.topic.clone(), partition), leader, followers)
    }

    /// generate test replica with assigned SPU
    fn replica(&self) -> Replica {
        let leader = self.leader_id();
        let mut followers = vec![self.leader_id()];
        for i in 0..self.followers {
            followers.push(self.follower_id(i));
        }
        Replica::new((self.topic.clone(), 0), leader, followers)
    }

    //. generate source replica
    fn source_replica(&self) -> Replica {
        let mut replica = self.replica();

        replica.mirror = Some(PartitionMirrorConfig::Source(SourcePartitionConfig {
            upstream_cluster: self.upstream_cluster.clone(),
            target_spu: self.base_spu_id,
        }));
        replica
    }

    fn target_replica(&self, remote_cluster_name: &str, partition: PartitionId) -> Replica {
        let mut replica = self.replica_with_partition(partition);

        replica.mirror = Some(PartitionMirrorConfig::Target(TargetPartitionConfig {
            remote_cluster: remote_cluster_name.to_string(),
            source_replica: ReplicaKey::new(self.source_topic.clone(), 0u32).to_string(),
        }));

        replica
    }

    fn spu_specs(&self) -> Vec<SpuSpec> {
        let mut leader_spec = SpuSpec::new_private_addr(
            self.leader_id(),
            self.leader_private_port(),
            self.host.clone(),
        );
        leader_spec.public_endpoint = IngressPort {
            port: self.leader_public_port(),
            ingress: vec![IngressAddr::from_host(self.host.clone())],
            ..Default::default()
        };
        let mut specs = vec![leader_spec];

        for i in 0..self.followers {
            specs.push(SpuSpec::new_private_addr(
                self.follower_id(i),
                self.follower_port(i),
                self.host.clone(),
            ));
        }
        specs
    }

    pub(crate) fn leader_config(&self) -> SpuConfig {
        let mut config = SpuConfig::default();
        config.log.base_dir = self.base_dir.clone();
        config.id = self.base_spu_id;
        config.private_endpoint = format!("{}:{}", self.host, self.base_port);
        config
    }

    pub(crate) async fn leader_ctx(&self) -> DefaultSharedGlobalContext {
        let leader_config = self.leader_config();

        let gctx = GlobalContext::new_shared_context(leader_config);
        gctx.spu_localstore().sync_all(self.spu_specs());
        gctx.sync_follower_update().await;

        gctx
    }

    /// creates mirror source ctx and returns source replica
    pub(crate) async fn init_mirror_source(
        &self,
    ) -> (DefaultSharedGlobalContext, LeaderReplicaState<FileReplica>) {
        let replica = self.source_replica();

        let gctx = self.leader_ctx().await;
        gctx.replica_localstore().sync_all(vec![replica.clone()]);

        gctx.upstream_cluster_localstore()
            .sync_all(vec![UpstreamCluster {
                name: self.upstream_cluster.to_owned(),
                spec: UpstreamClusterSpec {
                    source_id: self.upstream_cluster.clone(),
                    target: UpstreamTarget {
                        endpoint: self.upstream_port.clone(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            }]);

        let leader_replica = gctx
            .leaders_state()
            .add_leader_replica(&gctx, replica.clone(), gctx.status_update_owned())
            .await
            .expect("leader");

        (gctx, leader_replica)
    }

    /// creates mirror target ctx
    /// for each of the remote cluster, we create target replicas
    pub(crate) async fn init_mirror_target(&self) -> DefaultSharedGlobalContext {
        let gctx: std::sync::Arc<GlobalContext<FileReplica>> = self.leader_ctx().await;

        let mut replicas = vec![];
        let mut remote_clusters = vec![];

        for (partition_id, remote_cluster) in self.remote_clusters.iter().enumerate() {
            let replica = self.target_replica(remote_cluster, partition_id as PartitionId);

            let remote_cluster = RemoteCluster {
                name: remote_cluster.clone(),
                spec: RemoteClusterSpec::default(),
            };

            let _ = gctx
                .leaders_state()
                .add_leader_replica(&gctx, replica.clone(), gctx.status_update_owned())
                .await
                .expect("leader");

            replicas.push(replica);
            remote_clusters.push(remote_cluster);
        }

        gctx.replica_localstore().sync_all(replicas);
        gctx.remote_cluster_localstore().sync_all(remote_clusters);

        gctx
    }

    // // add upstream cluster
    // pub(crate) fn add_remote_clusters(
    //     &self,
    //     gctx: &DefaultSharedGlobalContext,
    //     remote_clusters: Vec<RemoteCluster>,
    // ) {
    //     gctx.remote_cluster_localstore().sync_all(remote_clusters);
    // }
}

impl ReplicaConfigBuilder {
    // generate config with specific sub directory
    pub(crate) fn generate(&mut self, child_dir: &str) -> ReplicaConfig {
        let mut test_config = self.build().unwrap();
        let test_path = test_config.base_dir.join(child_dir);
        ensure_clean_dir(&test_path);
        test_config.base_dir = test_path;
        test_config
    }
}

#[test]
fn replica_builder_test() {
    let test = ReplicaConfig::builder()
        .followers(2)
        .base_spu_id(6001)
        .build()
        .expect("build");

    assert_eq!(test.leader_id(), 6001);
    assert_eq!(test.follower_id(0), 6002);
    assert_eq!(test.follower_id(1), 6003);
    assert_eq!(test.leader_private_port(), 9000);
    assert_eq!(test.leader_public_port(), 9001);
    assert_eq!(test.follower_port(0), 9002);
}

#[test]
fn replica_spu_specs_test() {
    let test = ReplicaConfig::builder().build().expect("build");

    assert_eq!(
        test.spu_specs(),
        vec![SpuSpec {
            id: 5001,
            public_endpoint: IngressPort {
                port: 9001,
                ingress: vec![IngressAddr::from_host("127.0.0.1".to_owned())],
                ..Default::default()
            },
            private_endpoint: Endpoint {
                port: 9000,
                host: "127.0.0.1".to_owned(),
                ..Default::default()
            },
            ..Default::default()
        }]
    );
}

#[fluvio_future::test()]
async fn replica_leader_write_test() {
    let builder = ReplicaConfig::builder().generate("just_leader");

    let (leader_gctx, leader_replica) = builder.init_mirror_source().await;

    assert_eq!(leader_replica.leo(), 0);
    assert_eq!(leader_replica.hw(), 0);

    let status = leader_gctx.status_update().remove_all().await;
    assert!(!status.is_empty());
    let lrs = &status[0];
    assert_eq!(lrs.id, ("temp", 0).into());
    assert_eq!(lrs.leader.spu, 5001);
    assert_eq!(lrs.leader.hw, 0);
    assert_eq!(lrs.leader.leo, 0);

    // write records
    leader_replica
        .write_record_set(
            &mut create_raw_recordset(2),
            leader_gctx.follower_notifier(),
        )
        .await
        .expect("write");

    assert_eq!(leader_replica.leo(), 2);
    assert_eq!(leader_replica.hw(), 2);

    let status = leader_gctx.status_update().remove_all().await;
    assert!(!status.is_empty());
    let lrs = &status[0];
    assert_eq!(lrs.id, ("temp", 0).into());
    assert_eq!(lrs.leader.spu, 5001);
    assert_eq!(lrs.leader.hw, 2);
    assert_eq!(lrs.leader.leo, 2);
}
