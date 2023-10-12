pub mod producer;
pub mod consumer;
pub mod util;

use clap::Parser;

use tokio::spawn;
use fluvio_test_derive::fluvio_test;
use fluvio_test_case_derive::MyTestCase;

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq, MyTestCase)]
#[command(name = "Fluvio Concurrent Test")]
pub struct ConcurrentTestOption {}

#[fluvio_test(topic = "test-bug")]
pub fn concurrent(mut test_driver: TestDriver, mut test_case: TestCase) {
    println!("Testing concurrent consumer and producer");
    let option: MyTestCase = test_case.into();

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (sender, receiver) = std::sync::mpsc::channel();
        spawn(consumer::consumer_stream(
            test_driver.clone(),
            option.clone(),
            receiver,
        ));
        producer::producer(&test_driver, option, sender).await;
    });
}
