#![feature(async_await)]

mod test_runner;

pub use test_runner::SpuTestRunner;

use std::io::Error as IoError;

use futures::Future;
use futures::channel::mpsc::Receiver;

use metadata::spu::SpuSpec;
use metadata::spu::Endpoint as MetadatEndPoint;
use kf_socket::KfSocketError;
use types::SpuId;

/// trait for driving system test
pub trait FlvSystemTest: Sized {

    type EnvGenerator: TestGenerator;

    // return environment generator
    fn env_configuration(&self) -> Self::EnvGenerator;

    fn followers(&self) -> usize;

    type TestResponseFuture: Send +  Future<Output=Result<SpuTestRunner<Self>,KfSocketError>>;

    fn main_test(self,runner: SpuTestRunner<Self>) -> Self::TestResponseFuture;
}

pub trait SpuServer {

    type ShutdownFuture: Send +  Future<Output=Result<(),KfSocketError>>;
    fn run_shutdown(self,shutdown_signal: Receiver<bool>) -> Self::ShutdownFuture;

    fn id(&self) -> SpuId;

    fn spec(&self) -> &SpuSpec;

    
}


pub trait TestGenerator {

    type SpuServer: SpuServer;

    fn base_port(&self) -> u16;

    fn base_id(&self) -> i32;    

    fn create_spu(&self, spu_index: u16) -> SpuSpec {

        let port = spu_index * 2 + self.base_port();
        
        SpuSpec {
            id: self.base_id() + spu_index as i32,
            public_endpoint: MetadatEndPoint {
                port,
                ..Default::default()  
            },
            private_endpoint: MetadatEndPoint {
                port: port + 1,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// create server with and start controller
    fn create_server(&self, spu: &SpuSpec) -> Result<Self::SpuServer, IoError>;
    
}
