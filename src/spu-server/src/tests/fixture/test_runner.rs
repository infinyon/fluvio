use std::time::Duration;
use std::sync::Arc;
use std::sync::RwLock;

use log::debug;
use futures::SinkExt;
use futures::future::join3;
use futures::future::join_all;
use futures::channel::mpsc::channel;
use futures::channel::mpsc::Sender;

use flv_future_core::sleep;
use kf_socket::KfSocketError;
use kf_socket::KfSink;
use kf_protocol::api::Offset;
use kf_protocol::api::RequestMessage;
use kf_protocol::message::produce::DefaultKfProduceRequest;
use kf_protocol::message::produce::DefaultKfPartitionRequest;
use kf_protocol::message::produce::DefaultKfTopicRequest;
use kf_protocol::message::fetch::DefaultKfFetchRequest;
use kf_protocol::message::fetch::FetchPartition;
use kf_protocol::message::fetch::KfFetchRequest;
use kf_protocol::message::fetch::FetchableTopic;
use kf_protocol::api::DefaultBatch;
use kf_protocol::api::DefaultRecord;
use internal_api::messages::UpdateAllSpusMsg;
use internal_api::messages::UpdateAllSpusContent;
use internal_api::messages::Replica;
use internal_api::UpdateSpuRequest;
use flv_metadata::partition::ReplicaKey;
use flv_types::SpuId;
use flv_metadata::spu::SpuSpec;

use crate::core::DefaultSharedGlobalContext;
use super::mock_sc::SharedScContext;
use super::SpuTest;
use super::SpuServer;

struct ScServerCtx {
    ctx: SharedScContext,
    sender: Sender<bool>,
}

pub struct SpuTestRunner<T> {
    client_id: String,
    spu_server_specs: Vec<SpuServer>,
    spu_server_ctx: Vec<DefaultSharedGlobalContext>,
    spu_senders: Vec<Sender<bool>>,
    sc_ctx: RwLock<Option<ScServerCtx>>,
    test: T,
}

impl<T> SpuTestRunner<T>
where
    T: SpuTest + Send + Sync + 'static,
{
    pub async fn run(client_id: String, test: T) -> Result<(), KfSocketError> {
        debug!("starting test harnerss");
        let generator = test.env_configuration();

        let mut spu_server_ctx = vec![];
        let mut server_futures = vec![];
        let mut spu_senders = vec![];
        let mut spu_server_specs = vec![];
        for i in 0..test.followers() + 1 {
            let spu_spec = generator.create_spu_spec(i as u16);
            let (sender, receiver) = channel::<bool>(1);
            let (server, ctx) = generator.create_spu_server(&spu_spec)?;
            server_futures.push(server.run_shutdown(receiver));
            spu_senders.push(sender);
            spu_server_ctx.push(ctx);
            spu_server_specs.push(spu_spec.into());
        }

        let runner = SpuTestRunner {
            client_id,
            spu_server_specs,
            spu_server_ctx,
            spu_senders,
            sc_ctx: RwLock::new(None),
            test,
        };

        let arc_runner = Arc::new(runner);

        let (sender, receiver) = channel::<bool>(1);
        let (sc_server_ctx, sc_server) = generator.create_sc_server(arc_runner.clone());

        arc_runner.set_sc_ctx(ScServerCtx {
            ctx: sc_server_ctx,
            sender,
        });
        join3(
            arc_runner.run_test(),
            join_all(server_futures),
            sc_server.run_shutdown(receiver),
        )
        .await;
        Ok(())
    }

    async fn run_test(self: Arc<Self>) {
        //  wait until controller start up
        sleep(Duration::from_millis(50)).await.expect("panic");

        debug!("starting custom test logic");
        self.test()
            .main_test(self.clone())
            .await
            .expect("test should run");
        self.terminate_server().await;
    }

    fn test(&self) -> &T {
        &self.test
    }

    // terminating server
    async fn terminate_server(&self) {
        // terminate servers
        for i in 0..self.spu_server_specs.len() {
            let server = &self.spu_server_specs[i];
            let mut sender = self.spu_sender(i);

            debug!("terminating server: {}", server.id());
            sender.send(true).await.expect("spu shutdown should work");
        }

        // terminate sc
        if let Some(mut sc_sender) = self.sc_sender() {
            debug!("terminating sc");
            sc_sender.send(true).await.expect("sc shutdown should work");
        }
    }

    fn set_sc_ctx(&self, ctx: ScServerCtx) {
        let mut lock = self.sc_ctx.write().unwrap();
        *lock = Some(ctx);
    }

    fn spu_sender(&self, spu: usize) -> Sender<bool> {
        self.spu_senders[spu].clone()
    }

    fn sc_sender(&self) -> Option<Sender<bool>> {
        let lock = self.sc_ctx.read().unwrap();
        lock.as_ref().map(|ctx| ctx.sender.clone())
    }

    pub fn leader(&self) -> &SpuServer {
        &self.spu_server_specs[0]
    }

    pub fn leader_spec(&self) -> &SpuSpec {
        self.leader().spec()
    }

    pub fn followers_count(&self) -> usize {
        self.spu_server_specs.len() - 1
    }

    pub fn follower_spec(&self, index: usize) -> &SpuSpec {
        self.spu_server_specs[index + 1].spec()
    }

    pub fn leader_gtx(&self) -> DefaultSharedGlobalContext {
        self.spu_server_ctx[0].clone()
    }

    pub fn follower_gtx(&self, index: usize) -> DefaultSharedGlobalContext {
        self.spu_server_ctx[index + 1].clone()
    }

    pub fn spu_metadata(&self) -> UpdateAllSpusContent {
        let mut spu_metadata = UpdateAllSpusContent::default();

        for server in &self.spu_server_specs {
            spu_metadata.mut_add_spu_content(server.spec());
        }

        spu_metadata
    }

    pub fn replica_ids(&self) -> Vec<SpuId> {
        self.spu_server_specs
            .iter()
            .map(|follower| follower.spec().id)
            .collect()
    }

    pub fn replica_metadata(&self, replica: &ReplicaKey) -> Replica {
        let leader_id = self.leader_spec().id;

        Replica::new(replica.clone(), leader_id, self.replica_ids())
    }

    pub async fn send_metadata_to_spu<'a>(
        &'a self,
        sink: &'a mut KfSink,
        target_spu: SpuId,
    ) -> Result<(), KfSocketError> {
        let mut spu_metadata = self.spu_metadata();

        for replica in self.test.replicas() {
            spu_metadata.add_replica_by_ref(self.replica_metadata(&replica));
        }
        for server in &self.spu_server_specs {
            let spu_id = server.spec().id;
            if spu_id == target_spu {
                let spu_req_msg = RequestMessage::new_request(UpdateSpuRequest::encode_request(
                    UpdateAllSpusMsg::with_content(spu_id, spu_metadata.clone()),
                ))
                .set_client_id(self.client_id.clone());
                debug!("sending spu metadata to spu: {}", spu_id);
                sink.send_request(&spu_req_msg).await?;
            }
        }

        Ok(())
    }

    /// create sample message
    pub fn create_producer_msg<S>(
        &self,
        msg: S,
        topic: S,
        partition: i32,
    ) -> RequestMessage<DefaultKfProduceRequest>
    where
        S: Into<String>,
    {
        let msg_string: String = msg.into();
        let record: DefaultRecord = msg_string.into();
        let mut batch = DefaultBatch::default();
        batch.records.push(record);

        let mut topic_request = DefaultKfTopicRequest::default();
        topic_request.name = topic.into();
        let mut partition_request = DefaultKfPartitionRequest::default();
        partition_request.partition_index = partition;
        partition_request.records.batches.push(batch);
        topic_request.partitions.push(partition_request);
        let mut req = DefaultKfProduceRequest::default();
        req.topics.push(topic_request);

        RequestMessage::new_request(req).set_client_id(self.client_id.clone())
    }

    pub fn create_fetch_request<S>(
        &self,
        offset: Offset,
        topic: S,
        partition: i32,
    ) -> RequestMessage<DefaultKfFetchRequest>
    where
        S: Into<String>,
    {
        let mut request: DefaultKfFetchRequest = KfFetchRequest::default();
        let mut part_request = FetchPartition::default();
        part_request.partition_index = partition;
        part_request.fetch_offset = offset;
        let mut topic_request = FetchableTopic::default();
        topic_request.name = topic.into();
        topic_request.fetch_partitions.push(part_request);

        request.topics.push(topic_request);

        RequestMessage::new_request(request).set_client_id("test_client")
    }
}
