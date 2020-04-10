
use types::SpuId;


#[derive(Debug)]
pub struct ReplicaLeaderConfig {

    spu_id: SpuId,
    topic: String,
    partition: i32,
}


impl ReplicaLeaderConfig {

    pub fn new(topic: String,partition: i32) -> Self
    {
        Self {
            topic,
            partition,
            spu_id: 0,
        }
    }



    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }


    pub fn spu_id(mut self,id: SpuId) -> Self {
        self.spu_id = id;
        self
    }
    
}
