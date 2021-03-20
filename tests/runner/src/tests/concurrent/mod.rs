pub mod producer;
pub mod consumer;
pub mod util;

use std::sync::Arc;
use std::any::Any;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_future::task::spawn;
use fluvio_integration_derive::fluvio_test;
use fluvio_test_util::test_meta::{EnvironmentSetup, TestOption, TestCase};

const PARTITION: i32 = 0;

#[derive(Debug, Clone)]
pub struct ConcurrentTestCase {
    pub environment: EnvironmentSetup,
    pub option: ConcurrentTestOption,
}

impl From<TestCase> for ConcurrentTestCase {
    fn from(test_case: TestCase) -> Self {
        let concurrent_option = test_case
            .option
            .as_any()
            .downcast_ref::<ConcurrentTestOption>()
            .expect("ConcurrentTestOption")
            .to_owned();
        ConcurrentTestCase {
            environment: test_case.environment,
            option: concurrent_option,
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
pub struct ConcurrentTestOption {}

impl TestOption for ConcurrentTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test(topic = "test-bug")]
pub async fn run(client: Arc<Fluvio>, mut test_case: TestCase) {
    test_concurrent_consume_produce(client, test_case.into()).await
}

pub async fn test_concurrent_consume_produce(client: Arc<Fluvio>, option: ConcurrentTestCase) {
    println!("Testing concurrent consumer and producer");
    let (sender, receiver) = std::sync::mpsc::channel();
    spawn(consumer::consumer_stream(
        client.clone(),
        option.clone(),
        receiver,
    ));
    producer::producer(client, option, sender).await;
}
