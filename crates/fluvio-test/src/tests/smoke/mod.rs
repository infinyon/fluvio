pub mod consume;
pub mod produce;
pub mod message;
pub mod offsets;
use crate::tests::smoke::consume::validate_consume_message_api;

use std::any::Any;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use clap::Parser;

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::{EnvironmentSetup};
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::async_process;

use fluvio_cli::TableFormatConfig;
use fluvio_controlplane_metadata::tableformat::{TableFormatSpec};

use fluvio_future::timer::sleep;

#[derive(Debug, Clone)]
pub struct SmokeTestCase {
    pub environment: EnvironmentSetup,
    pub option: SmokeTestOption,
}

impl From<TestCase> for SmokeTestCase {
    fn from(test_case: TestCase) -> Self {
        let smoke_option = test_case
            .option
            .as_any()
            .downcast_ref::<SmokeTestOption>()
            .expect("SmokeTestOption")
            .to_owned();
        Self {
            environment: test_case.environment,
            option: smoke_option,
        }
    }
}

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq)]
#[clap(name = "Fluvio Smoke Test")]
pub struct SmokeTestOption {
    #[clap(long)]
    pub use_cli: bool,
    #[clap(long, default_value = "1")]
    pub producer_iteration: u32,
    #[clap(long, default_value = "100")]
    pub producer_record_size: u32,
    #[clap(long)]
    pub consumer_wait: bool,
    #[clap(long)]
    pub connector_config: Option<PathBuf>,
    #[clap(long)]
    pub table_format_config: Option<PathBuf>,
    #[clap(long)]
    pub skip_consumer_validate: bool,
}

impl TestOption for SmokeTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

//inventory::submit! {
//    FluvioTest {
//        name: "smoke".to_string(),
//        test_fn: smoke,
//        validate_fn: validate_subcommand,
//    }
//}

//pub fn validate_subcommand(subcmd: Vec<String>) -> Box<dyn TestOption> {
//    Box::new(SmokeTestOption::from_iter(subcmd))
//}

#[fluvio_test(topic = "test")]
pub fn smoke(mut test_driver: FluvioTestDriver, mut test_case: TestCase) {
    let smoke_test_case: SmokeTestCase = test_case.into();

    // TableFormat test
    let maybe_table_format =
        if let Some(ref table_format_config) = smoke_test_case.option.table_format_config {
            let table_format_process = async_process!(
                async {
                    let time = SystemTime::now();
                    let config = TableFormatConfig::from_file(table_format_config)
                        .expect("TableFormat config load failed");
                    let table_format_spec: TableFormatSpec = config.into();
                    let name = table_format_spec.name.clone();

                    test_driver
                        .connect()
                        .await
                        .expect("Connecting to cluster failed");

                    let admin = test_driver.client().admin().await;

                    admin
                        .create(name.clone(), false, table_format_spec)
                        .await
                        .expect("TableFormat create failed");
                    println!("tableformat \"{}\" created", &name);

                    // Wait a moment then delete
                    sleep(Duration::from_secs(5)).await;

                    admin
                        .delete::<TableFormatSpec, _>(name.clone())
                        .await
                        .expect("TableFormat delete failed");
                    println!(
                        "tableformat \"{}\" deleted, took: {} seconds",
                        &name,
                        time.elapsed().unwrap().as_secs()
                    );
                },
                "tableformat"
            );

            Some(table_format_process)
            // Create a managed connector
        } else {
            None
        };

    // We're going to handle the `--consumer-wait` flag in this process
    let producer_wait = async_process!(
        async {
            let mut test_driver_consumer_wait = test_driver.clone();

            test_driver
                .connect()
                .await
                .expect("Connecting to cluster failed");
            println!("About to start producer test");

            let start_offset = produce::produce_message(test_driver, &smoke_test_case).await;

            // If we've passed in `--consumer-wait` then we should start the consumer after the producer
            if smoke_test_case.option.consumer_wait {
                test_driver_consumer_wait
                    .connect()
                    .await
                    .expect("Connecting to cluster failed");
                validate_consume_message_api(
                    test_driver_consumer_wait,
                    start_offset,
                    &smoke_test_case,
                )
                .await;
            }
        },
        "producer"
    );

    // By default, we should run the consumer and producer at the same time
    if !smoke_test_case.option.consumer_wait {
        let consumer_wait = async_process!(
            async {
                test_driver
                    .connect()
                    .await
                    .expect("Connecting to cluster failed");
                consume::validate_consume_message(test_driver, &smoke_test_case).await;
            },
            "consumer validation"
        );

        let _ = consumer_wait.join();
    }
    let _ = producer_wait.join();

    if let Some(table_format_wait) = maybe_table_format {
        let _ = table_format_wait.join();
    };
}
