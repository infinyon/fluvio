mod consume;
mod produce;
mod message;
mod concurrent;

use super::TestDriver;
use crate::TestOption;

use async_trait::async_trait;
use crate::tests::smoke::concurrent::test_concurrent_consume_produce;
use fluvio_system_util::bin::get_fluvio;
use fluvio_command::CommandExt;

/// simple smoke test runner which tests
pub struct SmokeTestRunner {
    option: TestOption,
}

impl SmokeTestRunner {
    pub fn new(option: TestOption) -> Self {
        Self { option }
    }
}

#[async_trait]
impl TestDriver for SmokeTestRunner {
    /// run tester
    async fn run(&self) {
        match &self.option.test.as_deref() {
            Some("concurrent") => {
                println!("Test concurrent produce and consume:");
                test_concurrent_consume_produce(&self.option).await;
            }
            _ => {
                println!("Test produce then consume:");
                test_produce_then_consume(&self.option).await;
            }
        }
    }
}

async fn test_produce_then_consume(option: &TestOption) {
    setup_produce_then_consume(option).await;
    let start_offsets = produce::produce_message(option).await;
    consume::validate_consume_message(option, start_offsets).await;
}

async fn setup_produce_then_consume(option: &TestOption) {
    let topic_name = option.topic_name(0);
    println!("creating test topic: <{}>", topic_name);
    let mut command = get_fluvio().expect("fluvio not founded");
    command
        .arg("topic")
        .arg("create")
        .arg(topic_name)
        .arg("--replication")
        .arg(option.replication().to_string());
    if let Some(log) = &option.client_log {
        command.env("RUST_LOG", log);
    }

    let _output = command
        .result()
        .expect("fluvio topic create should succeed");
}
