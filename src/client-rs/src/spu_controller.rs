
use async_trait::async_trait;


use crate::ClientError;
use crate::ReplicaLeader;
use crate::query_params::ReplicaConfig;

#[async_trait]
pub trait SpuController
{
    type Leader: ReplicaLeader;
    type TopicMetadata;

    async fn find_leader_for_topic_partition(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<Self::Leader, ClientError>;


    async fn delete_topic(&mut self, topic: &str) -> Result<String,ClientError>;

    /// create topic, for now we return string in order simply interface.  Otherwise, we have to add associate type
    async fn create_topic(&mut self,topic: String, replica: ReplicaConfig,validate_only: bool) -> Result<String,ClientError>;

    async fn topic_metadata(&mut self,topics: Option<Vec<String>>) -> Result<Vec<Self::TopicMetadata>,ClientError>;
}

