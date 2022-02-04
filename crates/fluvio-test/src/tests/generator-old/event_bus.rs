use structopt::StructOpt;
use uuid::Uuid;
use tracing::debug;
use futures_lite::StreamExt;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::{EnvironmentSetup};
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::async_process;
use super::GeneratorTestCase;
use fluvio_future::task::run_block_on;
use fluvio::Offset;
use fluvio_test_util::test_meta::environment::EnvDetail;

use fluvio::{RecordKey};

pub async fn event_bus_proc(
    mut test_driver: TestDriver,
    option: GeneratorTestCase,
    parent_uuid: Uuid,
) {
    println!("[sync] parent async top (UUID: {})", parent_uuid.clone());

    //let mut env = EnvironmentSetup::default();
    //let mut env = option.environment.clone();

    let sync_topic = "sync";
    let command_topic = "command";
    let report_topic = "report";

    {

        //let (ref sync_topic, ref command_topic, ref report_topic) = if !option.option.debug {
        //    (
        //        format!(
        //            "{}-{}",
        //            option.option.prefix_sync_topic,
        //            parent_uuid.clone()
        //        ),
        //        format!(
        //            "{}-{}",
        //            option.option.prefix_command_topic,
        //            parent_uuid.clone()
        //        ),
        //        format!(
        //            "{}-{}",
        //            option.option.prefix_report_topic,
        //            parent_uuid.clone()
        //        ),
        //    )
        //} else {
        //    (
        //        format!("{}", option.option.prefix_sync_topic,),
        //        format!("{}", option.option.prefix_command_topic,),
        //        format!("{}", option.option.prefix_report_topic,),
        //    )
        //};

        //env.topic_name = Some(sync_topic.to_string());
        //test_driver.create_topic(&env).await.unwrap();

        //env.topic_name = Some(command_topic.to_string());
        //test_driver.create_topic(&env).await.unwrap();

        //env.topic_name = Some(report_topic.to_string());
        //test_driver.create_topic(&env).await.unwrap();
    }
    // L

    println!("[sync] Do a reconnect");

    test_driver
        .connect()
        .await
        .expect("Connecting to cluster failed");

    println!("[sync] Get consumer");
    let sync_consumer = test_driver.get_consumer(&sync_topic, 0).await;

    let mut sync_stream = sync_consumer
        .stream(Offset::from_end(0))
        .await
        .expect("Unable to open stream");

    let mut num_ready = 0;

    println!("[sync] enter loop");
    'sync: loop {
        if let Some(Ok(record_raw)) = sync_stream.next().await {
            println!("[sync] loop");
            let record: Vec<&str> = std::str::from_utf8(record_raw.as_ref())
                .unwrap()
                .split_whitespace()
                .collect();

            if record[0] == "ready" {
                num_ready += 1;
            } else {
                println!("[sync] Got: {:#?}", record[0]);
                println!("[sync] Got: {:#?}", record[0]);
                println!("[sync] Got: {:#?}", record[0]);
            }

            if num_ready >= option.option.producers {
                let producer = test_driver.create_producer("sync").await;

                // Send the start command
                println!("[sync] Sending start!");
                producer.send(RecordKey::NULL, "start").await.unwrap();
                break 'sync;
            } else {
                println!("Driver: {num_ready:#?} {:#?}", option.option.producers)
            }
        }
    }

    println!("parent async bottom (UUID: {})", parent_uuid.clone());
}
