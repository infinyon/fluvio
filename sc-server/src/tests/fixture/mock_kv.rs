use std::sync::Arc;

use log::debug;
use futures::SinkExt;
use futures::future::BoxFuture;
use futures::future::FutureExt;

use types::SpuId;
use error::ServerError;
use metadata::partition::ReplicaKey;
use utils::actions::Actions;
use utils::SimpleConcurrentHashMap;
use types::log_on_err;

use crate::core::auth_tokens::AuthTokenKV;
use crate::core::auth_tokens::AuthTokenAction;
use crate::core::auth_tokens::AuthTokenKvsAction;
use crate::core::topics::TopicAction;
use crate::core::partitions::PartitionAction;
use crate::core::partitions::PartitionKV;
use crate::core::partitions::PartitionKvsAction;
use crate::core::topics::TopicKV;
use crate::core::topics::TopicKvsAction;
use crate::core::spus::SpuAction;
use crate::core::spus::SpuKV;
use crate::core::KvMetadataService;
use crate::core::ScRequestSender;
use crate::core::ScRequest;

/// dummy kv store, all it does is maintain in memory represnetation of kv
///
///
pub struct MockKVStore {
    spus: SimpleConcurrentHashMap<SpuId, SpuKV>,
    topics: SimpleConcurrentHashMap<String, TopicKV>,
    partitions: SimpleConcurrentHashMap<ReplicaKey, PartitionKV>,
}

impl MockKVStore {
    fn new() -> Self {
        MockKVStore {
            spus: SimpleConcurrentHashMap::new(),
            topics: SimpleConcurrentHashMap::new(),
            partitions: SimpleConcurrentHashMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct SharedKVStore {
    kv: Arc<MockKVStore>,
    sc_request: ScRequestSender,
}

impl SharedKVStore {
    pub fn new(sc_request: ScRequestSender) -> Self {
        SharedKVStore {
            kv: Arc::new(MockKVStore::new()),
            sc_request,
        }
    }

    pub fn spus(&self) -> &SimpleConcurrentHashMap<SpuId, SpuKV> {
        &self.kv.spus
    }

    /// raw inserting spuys
    pub fn insert_spus(&self, spus: Vec<SpuKV>) {
        let mut write_lock = self.kv.spus.write();
        for spu in spus {
            write_lock.insert(spu.id(), spu);
        }
    }

    pub fn insert_topics(&self, topics: Vec<(String, TopicKV)>) {
        let mut write_lock = self.kv.topics.write();
        for (name, topic) in topics {
            write_lock.insert(name, topic);
        }
    }

    pub fn insert_partitions(&self, partitions: Vec<(ReplicaKey, PartitionKV)>) {
        let mut write_lock = self.kv.partitions.write();
        for (key, partition) in partitions {
            write_lock.insert(key, partition);
        }
    }

    // send all values to sc
    pub async fn update_all(mut self) {
        debug!("sending all metadata to controller");

        let auth_token_actions: Actions<AuthTokenAction> = Actions::default();

        let mut spu_actions: Actions<SpuAction> = Actions::default();
        for (_, spu) in self.kv.spus.read().iter() {
            let name = format!("spu-{}", spu.id());
            spu_actions.push(SpuAction::AddSpu(name, spu.clone()));
        }

        let mut topic_actions: Actions<TopicAction> = Actions::default();
        for (name, topic) in self.kv.topics.read().iter() {
            topic_actions.push(TopicAction::AddTopic(name.clone(), topic.clone()));
        }

        let mut partition_actions: Actions<PartitionAction> = Actions::default();
        for (key, partition) in self.kv.partitions.read().iter() {
            partition_actions.push(PartitionAction::AddPartition(
                key.clone(),
                partition.clone(),
            ));
        }

        self.sc_request
            .send(ScRequest::UpdateAll(
                auth_token_actions,
                spu_actions,
                topic_actions,
                partition_actions,
            ))
            .await
            .expect("expect should work");
    }


    async fn update_auth_token(&self, _name: String, auth_token: AuthTokenKV) -> Result<(), ServerError> {
        debug!("update auth_token {:#?}", auth_token);
        Ok(())
    }

    async fn add_partition(&self, _name: ReplicaKey, partition: PartitionKV) -> Result<(), ServerError> {
        debug!("add partition {:#?}", partition);
        //self.0.spus.insert(spu.id(),spu);
        Ok(())
    }

    async fn update_partition(&self, _name: ReplicaKey, partition: PartitionKV) -> Result<(), ServerError> {
        debug!("update partition {:#?}", partition);
        //self.0.spus.insert(spu.id(),spu);
        Ok(())
    }

    async fn update_topic(&self, _name: String, topic: TopicKV) -> Result<(), ServerError> {
        debug!("update topic {:#?}", topic);
        Ok(())
    }
}

impl KvMetadataService for SharedKVStore {
    type ResponseFuture = BoxFuture<'static, Result<(), ServerError>>;

    /// update kvs
    /// 
     fn process_partition_actions(&self,partition_kvs_actions: Actions<PartitionKvsAction>) -> Self::ResponseFuture
    {
        debug!(
            "KVS-SND[partition]: {} actions",
            partition_kvs_actions.count()
        );

        let service = self.clone();

        async move {

            for action in partition_kvs_actions.iter() {
                match action {
                    PartitionKvsAction::AddPartition(name, partition) => {
                        log_on_err!(
                            service
                                .add_partition(name.clone(), partition.clone())
                                .await
                        );
                    }
                    PartitionKvsAction::UpdatePartitionStatus(name, partition) => {
                        log_on_err!(
                            service
                                .update_partition(name.clone(), partition.clone())
                                .await
                        );
                    }
                }
            }
            Ok(())

        }.boxed()
        
    }

    /// update kvs
    fn update_spu(&self, spu: SpuKV) -> Self::ResponseFuture {
        debug!("updating kv spu {:#?}", spu);
        let name = format!("spu-{}", spu.id());
        let mut spu_actions: Actions<SpuAction> = Actions::default();
        match self.kv.spus.insert(spu.id(), spu.clone()) {
            Some(old_spu) => spu_actions.push(SpuAction::ModSpu(name, spu, old_spu)),
            None => spu_actions.push(SpuAction::AddSpu(name, spu)),
        };
        let mut sc_sender = self.sc_request.clone();
        async move {
            sc_sender
                .send(ScRequest::UpdateSPUs(spu_actions))
                .await
                .expect("send");
            Ok(())
        }
            .boxed()
    }


    fn process_topic_actions(&self, _actions: Actions<TopicKvsAction>) -> Self::ResponseFuture {
        async {
            Ok(())
        }.boxed()
    }

    fn process_auth_actions(&self, _actions: Actions<AuthTokenKvsAction>) -> Self::ResponseFuture {
        async {
            Ok(())
        }.boxed()
    }
    
}
