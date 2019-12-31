// test internal services
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use futures::future::BoxFuture;
use futures::future::FutureExt;

use flv_future_core::test_async;
use flv_future_core::sleep;
use kf_socket::KfSocketError;
use types::SpuId;

use crate::tests::fixture::ScTestRunner;
use crate::tests::fixture::ScTest;
use crate::tests::fixture::TestGenerator;

struct ReplicationTest {}

impl ScTest for ReplicationTest {
    type ResponseFuture = BoxFuture<'static, Result<(), KfSocketError>>;

    fn env_configuration(&self) -> TestGenerator {
        TestGenerator::default()
            .set_base_id(7000)
            .set_base_port(7000)
            .set_total_spu(2)
            .set_init_spu(2)
    }

    fn topics(&self) -> Vec<(String, Vec<Vec<SpuId>>)> {
        vec![("test".to_owned(), vec![vec![7000, 7001]])]
    }

    /// spin up spu and down.
    fn main_test(&self, _runner: Arc<ScTestRunner<ReplicationTest>>) -> Self::ResponseFuture {
        async move {
            sleep(Duration::from_millis(100)).await.expect("panic");

            debug!("spu server: 7000 is up, let's make sure sc and spu done it's work");
            Ok(())
        }
        .boxed()
    }
}

/// test spu online and offline
#[test_async]
async fn replication_test() -> Result<(), KfSocketError> {
    let test = ReplicationTest {};
    ScTestRunner::run("replication test".to_owned(), test)
        .await
        .expect("test runner should not failer");
    Ok(())
}
