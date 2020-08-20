use std::time::Duration;
use std::sync::Arc;

use tracing::debug;
use futures::SinkExt;
use futures::future::join;
use futures::channel::mpsc::Sender;

use flv_future_core::sleep;
use kf_socket::KfSocketError;
use fluvio_types::SpuId;
use fluvio_metadata::topic::TopicSpec;
use fluvio_metadata::topic::TopicStatus;
use fluvio_metadata::topic::TopicResolution;
use fluvio_metadata::partition::ReplicaKey;
use utils::SimpleConcurrentHashMap;

use crate::core::common::test_fixtures::create_spu;
use crate::core::spus::SpuKV;
use crate::core::topics::TopicKV;
use crate::core::partitions::PartitionKV;

use super::ScTest;
use super::SharedSpuContext;
use super::SpuSpec;
use super::ScClient;
use super::TestGenerator;

pub struct SpuRunContext {
    pub sender: Sender<bool>,
    pub ctx: SharedSpuContext,
}
pub struct ScTestRunner<T> {
    client_id: String,
    sc_server: ScClient,
    spu_ctxs: SimpleConcurrentHashMap<SpuId, SpuRunContext>,
    test: T,
    generator: TestGenerator,
}

impl<T> ScTestRunner<T>
where
    T: ScTest + Send + Sync + 'static,
{
    pub async fn run(client_id: String, test: T) -> Result<(), KfSocketError> {
        debug!("starting sc test harness");
        let generator = test.env_configuration();

        let ((private_server, receiver_private), sc_server) = generator.create_sc_server();
        let runner = ScTestRunner {
            client_id,
            test,
            sc_server,
            generator,
            spu_ctxs: SimpleConcurrentHashMap::new(),
        };

        let arc_runner = Arc::new(runner);

        let mut spu_ctx = vec![];

        let generator = arc_runner.generator();
        debug!("starting init spu servers: {}", generator.initial_spu());
        for i in 0..generator.initial_spu() {
            let (ctx, sender) = generator.run_server_with_index(i, arc_runner.clone());
            spu_ctx.push((ctx.id(), SpuRunContext { sender, ctx }));
        }
        arc_runner.set_spu_ctx(spu_ctx);

        join(
            arc_runner.run_test(),
            private_server.run_shutdown(receiver_private),
        )
        .await;
        Ok(())
    }

    async fn run_test(self: Arc<Self>) {
        //  wait until controller start u
        self.send_initial_metadata_to_sc_controller().await;
        debug!("starting main test: waiting 10 ms");
        sleep(Duration::from_millis(10)).await.expect("panic");
        self.test()
            .main_test(self.clone())
            .await
            .expect("test should run");
        self.terminate_server().await;
    }

    pub fn test(&self) -> &T {
        &self.test
    }

    pub fn generator(&self) -> &TestGenerator {
        &self.generator
    }

    pub fn spu_ctxs(&self) -> &SimpleConcurrentHashMap<SpuId, SpuRunContext> {
        &self.spu_ctxs
    }

    pub fn sc_client(&self) -> &ScClient {
        &self.sc_server
    }

    // terminating server
    async fn terminate_server(&self) {
        self.sc_server.terminate_private_server().await;
        let sender_ctx = self.spu_senders();
        debug!("start terminating mock spu servers: {}", sender_ctx.len());
        for spu in sender_ctx {
            let (spec, mut sender) = spu;
            debug!("terminating mock spu server: {}", spec.id);
            sender.send(true).await.expect("spu shutdown should work");
        }
    }

    fn spu_senders(&self) -> Vec<(SpuSpec, Sender<bool>)> {
        let mut senders = vec![];
        let lock = self.spu_ctxs.read();
        for (_, ctx) in lock.iter() {
            senders.push((ctx.ctx.spec().clone(), ctx.sender.clone()));
        }
        senders
    }

    fn set_spu_ctx(&self, ctxs: Vec<(SpuId, SpuRunContext)>) {
        let mut lock = self.spu_ctxs.write();
        for (id, ctx) in ctxs {
            lock.insert(id, ctx);
        }
    }

    fn spu_specs(&self) -> Vec<SpuKV> {
        let mut sc_specs = vec![];
        let ctx_read_lock = self.spu_ctxs.read();
        for (_, spu_ctx) in ctx_read_lock.iter() {
            let spu_spec = spu_ctx.ctx.spec();
            let sc_spu_spec = create_spu(spu_spec.id, "123556", false, None);
            sc_specs.push(sc_spu_spec);
        }
        sc_specs
    }

    pub async fn send_initial_metadata_to_sc_controller(&self) {
        debug!("sending metadata to sc: waiting 5ms to spin up tests");
        sleep(Duration::from_millis(5)).await.expect("panic");
        debug!("populating store value with initial ");
        // populate spu
        let kv_store = self.sc_server.kv_store();

        let mut spu_specs = vec![];
        debug!("metadata spu count: {}", self.generator().total_spu());
        for i in 0..self.generator().total_spu() {
            let spu_spec = self.generator().create_spu_spec(i as u16);
            let spu_id = spu_spec.id;
            debug!("initial spu: {}", spu_id);
            let sc_spu_spec = create_spu(spu_id, "123556", false, None);
            spu_specs.push(sc_spu_spec);
        }
        kv_store.insert_spus(spu_specs);

        let mut topics = vec![];
        let mut partitions = vec![];

        for (topic, replica_map) in self.test.topics() {
            let status = TopicStatus::new(TopicResolution::Ok, replica_map.clone(), "".to_owned());

            let replica_len = if replica_map.len() > 0 {
                replica_map[0].len() as i32
            } else {
                0
            };
            topics.push((
                topic.clone(),
                TopicKV::new(TopicSpec::new_computed(1, replica_len, None), status),
            ));

            for replicas in replica_map {
                partitions.push((
                    ReplicaKey::new(topic.clone(), 0),
                    PartitionKV::with_replicas(0, replicas),
                ));
            }
        }

        kv_store.insert_topics(topics);
        kv_store.insert_partitions(partitions);

        // send
        kv_store.update_all().await;
    }
}
