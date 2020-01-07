mod client;
mod error;
mod spu;
mod sc;
mod kf;
mod spu_controller;
mod leader;
pub mod profile;
pub mod query_params;

pub use client::ClientConfig;
pub use client::Client;
pub use error::ClientError;
pub use spu::SpuLeader;
pub use spu::Spu;
pub use sc::ScClient;
pub use kf::KfClient;
pub use kf::KfLeader;
pub use spu_controller::SpuController;
pub use leader::ReplicaLeader;


use types::socket_helpers::ServerAddress;
use types::SpuId;

#[derive(Debug)]
pub struct LeaderConfig {

    spu_id: SpuId,
    addr: ServerAddress,
    client_id: String,
    topic: String,
    partition: i32
}



impl LeaderConfig {

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


    fn as_client_config(&self) -> ClientConfig<String> {
        ClientConfig::new(self.addr.to_string())
            .client_id(self.client_id.clone()) 
    }


    
}
