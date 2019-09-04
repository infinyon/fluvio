mod spg_operator;
mod conversion;
mod spg_group;

use metadata::spu::SpuSpec; 
use metadata::topic::TopicSpec;
use metadata::partition::PartitionSpec;

use crate::core::WSChangeDispatcher;
use crate::core::WSChangeChannel;
use crate::k8::SharedK8Client;
use crate::core::ShareLocalStores;
use crate::core::spus::K8SpuChangeDispatcher;
use crate::core::spus::SharedSpuLocalStore;
use crate::core::partitions::K8PartitionChangeDispatcher;
use crate::core::topics::K8TopicChangeDispatcher;
use spg_operator::SpgOperator;

use self::conversion::convert_cluster_to_statefulset;
use self::conversion::generate_service;
use self::spg_group::SpuGroupObj;
use self::spg_group::SpuValidation;

pub struct K8AllChangeDispatcher {
    spu: K8SpuChangeDispatcher,
    topic: K8TopicChangeDispatcher,
    partition: K8PartitionChangeDispatcher
}

impl K8AllChangeDispatcher {

    pub fn new(client: SharedK8Client,namespace: String,local_stores: ShareLocalStores) -> Self {

        Self {
            spu: K8SpuChangeDispatcher::new(namespace.clone(),client.clone(),local_stores.spus().clone()),
            topic: K8TopicChangeDispatcher::new(namespace.clone(),client.clone(),local_stores.topics().clone()),
            partition: K8PartitionChangeDispatcher::new(namespace.clone(),client.clone(),local_stores.partitions().clone())
        }
    }

    pub fn run(self) {
        self.spu.run();
        self.topic.run();
        self.partition.run();
    }


}


impl WSChangeDispatcher for K8AllChangeDispatcher {

     fn create_spu_channel(&mut self) -> WSChangeChannel<SpuSpec> {
         self.spu.create_channel()
     }

    fn create_topic_channel(&mut self) -> WSChangeChannel<TopicSpec> {
        self.topic.create_channel()
    }

    fn create_partition_channel(&mut self) -> WSChangeChannel<PartitionSpec>{
        self.partition.create_channel()
    }
}



pub fn run_spg_operator(
    client: SharedK8Client,
    namespace: String,
    spu_store: SharedSpuLocalStore
) {
    SpgOperator::new(client,namespace,spu_store).run();
}

