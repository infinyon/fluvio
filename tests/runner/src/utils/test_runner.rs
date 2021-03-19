#[allow(unused_imports)]
use fluvio_command::CommandExt;
use crate::test_meta::{EnvDetail, EnvironmentSetup};
use fluvio::Fluvio;
use std::sync::Arc;
use fluvio::metadata::topic::TopicSpec;

pub struct FluvioTest {}

impl FluvioTest {
    pub async fn create_topic(client: Arc<Fluvio>, option: &EnvironmentSetup) -> Result<(), ()> {
        println!("Creating the topic: {}", &option.topic_name);

        let mut admin = client.admin().await;
        let topic_spec = TopicSpec::new_computed(1, option.replication() as i32, None);

        let topic_create = admin
            .create(option.topic_name.clone(), false, topic_spec)
            .await;

        if let Ok(_) = topic_create {
            println!("topic \"{}\" created", option.topic_name);
        } else {
            println!("topic \"{}\" already exists", option.topic_name);
        }

        Ok(())
    }
}
