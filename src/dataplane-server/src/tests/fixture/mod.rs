mod generator;
mod spu_client;
mod mock_sc;
mod test_runner;

pub(crate) use generator::TestGenerator;
pub(crate) use test_runner::SpuTestRunner;
pub(crate) use spu_client::SpuServer;

use std::sync::Arc;

use futures::Future;

use kf_socket::KfSocketError;
use fluvio_metadata::partition::ReplicaKey;

/// Customize System Test
pub trait SpuTest: Sized {
    type ResponseFuture: Send + Future<Output = Result<(), KfSocketError>>;

    /// environment configuration
    fn env_configuration(&self) -> TestGenerator;

    /// number of followers
    fn followers(&self) -> usize;

    /// replicas.  by default, it's empty
    fn replicas(&self) -> Vec<ReplicaKey> {
        vec![]
    }

    /// main entry point
    fn main_test(&self, runner: Arc<SpuTestRunner<Self>>) -> Self::ResponseFuture;
}
