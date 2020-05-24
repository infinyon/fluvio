// test internal services
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use futures::SinkExt;

use flv_future_core::test_async;
use flv_future_core::sleep;
use kf_socket::KfSocketError;
use flv_types::SpuId;
use flv_metadata::spu::SpuResolution;

use crate::tests::fixture::ScTestRunner;
use crate::tests::fixture::ScTest;
use crate::tests::fixture::TestGenerator;

const BASE_ID: i32 = 7100;

struct SimpleInternalTest {}

impl ScTest for SimpleInternalTest {
    type ResponseFuture = BoxFuture<'static, Result<(), KfSocketError>>;

    fn env_configuration(&self) -> TestGenerator {
        TestGenerator::default()
            .set_base_id(BASE_ID)
            .set_base_port(BASE_ID as u16)
            .set_total_spu(1)
    }

    fn topics(&self) -> Vec<(String, Vec<Vec<SpuId>>)> {
        vec![("test".to_owned(), vec![vec![BASE_ID]])]
    }

    /// spin up spu and down.
    fn main_test(&self, runner: Arc<ScTestRunner<SimpleInternalTest>>) -> Self::ResponseFuture {
        let generator = runner.generator();
        let (ctx, mut spu0_terminator) = generator.run_server_with_index(0, runner.clone()); // 7000

        async move {
            sleep(Duration::from_millis(100)).await.expect("panic");

            debug!(
                "spu server: {} is up, let's make sure sc and spu done it's work",
                BASE_ID
            );
            let sc_server = runner.sc_client();
            let kv_store = sc_server.kv_store();
            {
                let lock = kv_store.spus().read();
                let spu = lock.get(&BASE_ID).expect("spu");
                assert_eq!(spu.status.resolution, SpuResolution::Online);
                let spu_lock = ctx.spus.read();
                let name = format!("spu-{}", BASE_ID);
                spu_lock.get(&name).expect("spu content");
            }

            // shutdown internal server
            debug!("shutting down spu {}", BASE_ID);
            spu0_terminator.send(true).await.expect("shutdown spu 0");
            debug!("ready to test down state");
            sleep(Duration::from_millis(20)).await.expect("panic");
            {
                let lock = kv_store.spus().read();
                let spu = lock.get(&BASE_ID).expect("spu");
                assert_eq!(spu.status.resolution, SpuResolution::Offline);
                let spu_lock = ctx.spus.read();
                let name = format!("spu-{}", BASE_ID);
                spu_lock.get(&name).expect("spu content");
            }
            Ok(())
        }
        .boxed()
    }
}

/// test spu online and offline
#[test_async]
async fn connection_test() -> Result<(), KfSocketError> {
    let test = SimpleInternalTest {};
    ScTestRunner::run("connection test".to_owned(), test)
        .await
        .expect("test runner should not failer");
    Ok(())
}
