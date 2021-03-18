#[allow(unused_imports)]
use fluvio_command::CommandExt;
use async_trait::async_trait;
use fluvio_system_util::bin::get_fluvio;
use crate::test_meta::TestCase;

pub async fn create_topic(option: &TestCase) -> Result<(), ()> {
    println!("Creating the topic: {}", &option.environment.topic_name);
    let mut command = get_fluvio().expect("Fluvio binary not found");
    command
        .arg("topic")
        .arg("create")
        .arg(&option.environment.topic_name)
        .arg("--replication")
        .arg(&option.environment.replication.to_string());
    if let Some(log) = &option.environment.client_log {
        command.env("RUST_LOG", log);
    }

    let _output = command
        .result()
        .expect("fluvio topic create should succeed");

    Ok(())
}

#[async_trait]
pub trait TestDriver {
    /// run tester
    async fn run(self);
}
