use std::collections::HashMap;
use std::time::SystemTime;

use tracing::{info, debug};

use fluvio_test_util::test_runner::test_driver::{TestDriver};
use fluvio_test_util::test_meta::environment::EnvDetail;
use fluvio::RecordKey;
use fluvio_command::CommandExt;
use fluvio::TopicProducerConfigBuilder;

use super::SmokeTestCase;
use super::message::*;
use super::offsets;

type Offsets = HashMap<String, i64>;

pub async fn produce_message(test_driver: TestDriver, test_case: &SmokeTestCase) -> Offsets {
    let offsets = offsets::find_offsets(&test_driver, test_case).await;

    let use_cli = test_case.option.use_cli;

    if use_cli {
        cli::produce_message_with_cli(test_case, offsets.clone()).await;
    } else {
        produce_message_with_api(test_driver, offsets.clone(), test_case.clone()).await;
    }

    offsets
}

pub async fn produce_message_with_api(
    test_driver: TestDriver,
    offsets: Offsets,
    test_case: SmokeTestCase,
) {
    let partition = test_case.environment.partition;
    let produce_iteration = test_case.option.producer_iteration;
    let topic_name = test_case.environment.base_topic_name();
    let delivery_semantic = test_case.environment.producer_delivery_semantic;

    println!(">> start producing message: {{ topic_name }}");
    println!("delivery semantic: {delivery_semantic}");

    for r in 0..partition {
        let base_offset = *offsets.get(&topic_name).expect("offsets");

        let config = TopicProducerConfigBuilder::default()
            .delivery_semantic(delivery_semantic)
            .build()
            .expect("failed to build config");
        let producer = test_driver
            .create_producer_with_config(&topic_name, config)
            .await;

        debug!(base_offset, "created producer");

        let mut chunk_time = SystemTime::now();
        for i in 0..produce_iteration {
            let offset = base_offset + i as i64;
            let message = generate_message(offset, &test_case);
            let len = message.len();
            info!(topic = %topic_name, iteration = i, "trying send");

            test_driver
                .send_count(&producer, RecordKey::NULL, message.clone())
                .await
                .unwrap_or_else(|_| {
                    panic!("send record failed for replication: {r} iteration: {i}")
                });

            if i % 100 == 0 {
                let elapsed_chunk_time = chunk_time.elapsed().clone().unwrap().as_secs_f32();
                println!("total records sent: {i} chunk time: {elapsed_chunk_time:.5} secs");
                chunk_time = SystemTime::now();
            }
            info!(
                "completed send iter: {}, offset: {},len: {}",
                topic_name, offset, len
            );
        }
        producer.flush().await.expect("flush");
    }

    println!("<< produce message completed");
}

mod cli {
    use super::*;

    use std::io::Write;
    use std::process::Stdio;
    use fluvio_future::timer::sleep;
    use std::time::Duration;
    use crate::get_binary;

    pub async fn produce_message_with_cli(test_case: &SmokeTestCase, offsets: Offsets) {
        println!("starting produce");

        let producer_iteration = test_case.option.producer_iteration;

        for i in 0..producer_iteration {
            produce_message_replication(i, &offsets, test_case);
            sleep(Duration::from_millis(10)).await
        }
    }

    fn produce_message_replication(iteration: u32, offsets: &Offsets, test_case: &SmokeTestCase) {
        let replication = test_case.environment.replication;

        for _i in 0..replication {
            produce_message_inner(iteration, offsets, test_case);
        }
    }

    fn produce_message_inner(_iteration: u32, offsets: &Offsets, test_case: &SmokeTestCase) {
        use std::io;
        let topic_name = test_case.environment.base_topic_name();

        let base_offset = *offsets.get(&topic_name).expect("offsets");

        info!(
            "produce cli message: {}, offset: {}",
            topic_name, base_offset
        );

        let mut child = get_binary("fluvio").expect("no fluvio");
        if let Some(log) = &test_case.environment.client_log {
            child.env("RUST_LOG", log);
        }
        child.stdin(Stdio::piped()).arg("produce").arg(&topic_name);
        println!("Executing: {}", child.display());
        let mut child = child.spawn().expect("no child");

        let stdin = child.stdin.as_mut().expect("Failed to open stdin");
        let msg = generate_message(base_offset, test_case);
        stdin
            .write_all(msg.as_slice())
            .expect("Failed to write to stdin");

        let output = child.wait_with_output().expect("Failed to read stdout");
        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();
        assert!(output.status.success());

        println!("send message of len {}", msg.len());
    }
}
