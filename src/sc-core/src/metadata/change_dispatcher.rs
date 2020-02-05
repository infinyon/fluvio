use flv_metadata::spu::SpuSpec;
use flv_metadata::topic::TopicSpec;
use flv_metadata::partition::PartitionSpec;
use k8_metadata_client::MetadataClient;
use k8_metadata_client::SharedClient;

use crate::core::WSChangeDispatcher;
use crate::core::WSChangeChannel;
use crate::core::ShareLocalStores;
use crate::core::spus::K8SpuChangeDispatcher;
use crate::core::partitions::K8PartitionChangeDispatcher;
use crate::core::topics::K8TopicChangeDispatcher;

pub struct K8AllChangeDispatcher<C> {
    spu: K8SpuChangeDispatcher<C>,
    topic: K8TopicChangeDispatcher<C>,
    partition: K8PartitionChangeDispatcher<C>,
}

impl<C> K8AllChangeDispatcher<C>
where
    C: MetadataClient + 'static,
{
    pub fn new(client: SharedClient<C>, namespace: String, local_stores: ShareLocalStores) -> Self {
        Self {
            spu: K8SpuChangeDispatcher::new(
                namespace.clone(),
                client.clone(),
                local_stores.spus().clone(),
            ),
            topic: K8TopicChangeDispatcher::new(
                namespace.clone(),
                client.clone(),
                local_stores.topics().clone(),
            ),
            partition: K8PartitionChangeDispatcher::new(
                namespace.clone(),
                client.clone(),
                local_stores.partitions().clone(),
            ),
        }
    }

    pub fn run(self) {
        self.spu.run();
        self.topic.run();
        self.partition.run();
    }
}

impl<C> WSChangeDispatcher for K8AllChangeDispatcher<C>
where
    C: MetadataClient + 'static,
{
    fn create_spu_channel(&mut self) -> WSChangeChannel<SpuSpec> {
        self.spu.create_channel()
    }

    fn create_topic_channel(&mut self) -> WSChangeChannel<TopicSpec> {
        self.topic.create_channel()
    }

    fn create_partition_channel(&mut self) -> WSChangeChannel<PartitionSpec> {
        self.partition.create_channel()
    }
}
