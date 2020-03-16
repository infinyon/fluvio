
use types::socket_helpers::ServerAddress;
use types::SpuId;

use crate::ClientConfig;

#[derive(Debug)]
pub struct ReplicaLeaderConfig {

    spu_id: SpuId,
    addr: ServerAddress,
    client_id: String,
    topic: String,
    partition: i32
}



impl ReplicaLeaderConfig {

    pub fn new<A>(addr: A,topic: String,partition: i32) -> Self
        where A: Into<ServerAddress>
    {
        Self {
            addr: addr.into(),
            topic,
            partition,
            spu_id: 0,
            client_id: "spu".to_owned()
        }
    }


    pub fn addr(&self) -> &ServerAddress {
        &self.addr
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }


    pub fn get_client_id(&self) -> &str {
        &self.client_id
    }
    
    pub fn client_id<S>(mut self,id: S) -> Self 
        where S: Into<String>
    {
        self.client_id = id.into();
        self
    }

    pub fn spu_id(mut self,id: SpuId) -> Self {
        self.spu_id = id;
        self
    }


    pub fn as_client_config(&self) -> ClientConfig<String> {
        ClientConfig::new(self.addr.to_string())
            .client_id(self.client_id.clone()) 
    }


    
}
