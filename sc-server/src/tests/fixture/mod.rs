 mod generator;
 mod test_runner;
 mod mock_spu;
 mod mock_kv;
 mod mock_cm;

pub use generator::TestGenerator;
pub use generator::ScClient;
pub use test_runner::ScTestRunner;
pub use mock_spu::SharedSpuContext;
pub use mock_spu::SpuSpec;
pub use mock_kv::SharedKVStore;
pub use mock_kv::MockKVStore;
pub use mock_cm::MockConnectionManager;

use std::sync::Arc;

use futures::Future;

use kf_socket::KfSocketError;
use types::SpuId;

/// Customize System Test
pub trait ScTest: Sized {

    type ResponseFuture: Send +  Future<Output=Result<(),KfSocketError>>;

    /// environment configuration
    fn env_configuration(&self) -> TestGenerator;

    fn topics(&self) -> Vec<(String,Vec<Vec<SpuId>>)> ;

    /// main entry point
    fn main_test(&self,runner: Arc<ScTestRunner<Self>>) -> Self::ResponseFuture;
}
