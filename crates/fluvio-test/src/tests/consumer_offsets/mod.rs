mod none;
mod auto;
mod manual;
mod option;
mod utils;

use anyhow::Result;
use clap::Parser;

use fluvio::consumer::OffsetManagementStrategy;
use fluvio_test_derive::fluvio_test;
use option::{ConsumerOffsetsTestCase, ConsumerOffsetsTestOption};

#[fluvio_test(async)]
pub async fn consumer_offsets(
    mut test_driver: Arc<FluvioTestDriver>,
    mut test_case: TestCase,
) -> TestResult {
    println!("Starting offset_management test");

    let test_case: ConsumerOffsetsTestCase = test_case.into();
    let topic_name = test_case.environment.base_topic_name();
    let partitions = test_case.environment.partition as usize;
    let option = test_case.option;

    if partitions == 1 {
        println!("running on single partition");
    } else {
        println!("running on multiple partitions");
    };

    test_driver.connect().await.expect("connected");
    let client = test_driver.client();

    match option.strategy {
        OffsetManagementStrategy::None => {
            println!("running test_strategy_none");
            none::test_strategy_none(client, &topic_name, partitions)
                .await
                .expect("test_strategy_none");
        }
        OffsetManagementStrategy::Manual => {
            manual::run_manual_test(
                client,
                &topic_name,
                partitions,
                option
                    .offset_start
                    .expect("offset start parameter required"),
            )
            .await
            .expect("run_manual_test");
        }
        OffsetManagementStrategy::Auto => match option.offset_flush {
            Some(flush) => {
                println!("running test_strategy_auto_periodic_flush");
                auto::flush::test_strategy_auto_periodic_flush(
                    client,
                    &topic_name,
                    partitions,
                    flush,
                )
                .await
                .expect("test_strategy_auto_periodic_flush");
            }
            None => {
                println!("running test_strategy_auto");
                auto::test_strategy_auto(client, &topic_name, partitions)
                    .await
                    .expect("test_strategy_auto");
            }
        },
    }
}
