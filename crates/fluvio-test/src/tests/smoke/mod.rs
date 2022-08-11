pub mod consume;
pub mod produce;
pub mod message;
pub mod offsets;
use crate::tests::smoke::consume::validate_consume_message_api;

use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use std::fs::File;
use std::io::Read;
use std::collections::BTreeMap;

use clap::Parser;
use tracing::debug;
use serde::{Deserialize};

use fluvio_test_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::{EnvironmentSetup};
use fluvio_test_util::test_meta::{TestOption, TestCase};
use fluvio_test_util::async_process;

use fluvio_cli::TableFormatConfig;
use fluvio_controlplane_metadata::tableformat::{TableFormatSpec};
use fluvio::metadata::{
    topic::{TopicSpec, TopicReplicaParam, ReplicaSpec},
    connector::{ManagedConnectorSpec, ManagedConnectorParameterValue, SecretString},
};
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
    #[clap(long)]
    pub skip_test_connector: bool,
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

    // If connector tests requested
    let maybe_connector = if !smoke_test_case.option.skip_test_connector {
        if let Some(ref connector_config) = smoke_test_case.option.connector_config {
            let connector_process = async_process!(
                async {
                    test_driver
                        .connect()
                        .await
                        .expect("Connecting to cluster failed");

                    // Add a connector CRD
                    let admin = test_driver.client().admin().await;
                    // Create a managed connector
                    let config = ConnectorConfig::from_file(&connector_config).unwrap();
                    let spec: ManagedConnectorSpec = config.clone().into();
                    let name = spec.name.clone();

                    debug!("creating managed_connector: {}, spec: {:#?}", name, spec);

                    // If the managed connector wants its topic created, don't fail if it already exists
                    if config.create_topic {
                        println!("Attempt to create connector's topic");
                        let topic_spec: TopicSpec =
                            ReplicaSpec::Computed(TopicReplicaParam::new(1, 1, false)).into();
                        debug!("topic spec: {:?}", topic_spec);
                        admin
                            .create(config.topic.clone(), false, topic_spec)
                            .await
                            .unwrap_or(());
                    }

                    // If the connector already exists, don't fail
                    println!("Attempt to create connector");
                    admin
                        .create(name.to_string(), false, spec)
                        .await
                        .unwrap_or(());

                    // Build a new SmokeTestCase so we can use the consumer verify
                    // Ending after a static number of records received
                    let new_smoke_test_case = SmokeTestCase {
                        environment: EnvironmentSetup {
                            topic_name: Some(config.topic.clone()),
                            tls: smoke_test_case.environment.tls,
                            tls_user: smoke_test_case.environment.tls_user(),
                            spu: smoke_test_case.environment.spu,
                            replication: smoke_test_case.environment.replication,
                            partition: smoke_test_case.environment.partition,
                            ..Default::default()
                        },
                        option: SmokeTestOption {
                            skip_consumer_validate: true,
                            producer_iteration: 10,
                            connector_config: smoke_test_case.option.connector_config.clone(),
                            ..Default::default()
                        },
                    };

                    // Verify that connector is creating data
                    let start_offset =
                        offsets::find_offsets(&test_driver, &new_smoke_test_case.clone()).await;
                    let start = start_offset.get(&config.topic.clone()).expect("offsets");

                    println!("Verify connector is creating data: (start: {})", start);

                    const CI_TIME: u64 = 90;
                    const DEV_TIME: u64 = 10;

                    let wait_sec = if std::env::var("CI").is_ok() {
                        CI_TIME
                    } else {
                        DEV_TIME
                    };

                    println!("Waiting {} seconds to let connector write", &wait_sec);
                    sleep(Duration::from_secs(wait_sec)).await;

                    let check_offset =
                        offsets::find_offsets(&test_driver, &new_smoke_test_case.clone()).await;
                    let check = check_offset.get(&config.topic.clone()).expect("offsets");

                    if check > start {
                        println!("Connector is receiving data: (check: {})", check)
                    } else {
                        panic!("Connector not receiving data")
                    };

                    println!("Run consume test against connector topic");
                    validate_consume_message_api(
                        test_driver,
                        start_offset,
                        &new_smoke_test_case.clone(),
                    )
                    .await;
                },
                "connector"
            );

            Some(connector_process)

            // Wait a few seconds to allow the connector a chance to deploy and store data
        } else {
            None
        }
    } else {
        println!("Skipping test-connector tests, by request");
        None
    };

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

    if let Some(connector_wait) = maybe_connector {
        let _ = connector_wait.join();
    };

    if let Some(table_format_wait) = maybe_table_format {
        let _ = table_format_wait.join();
    };
}

// Copied from CLI Connector create
#[derive(Debug, Deserialize, Clone)]
pub struct ConnectorConfig {
    name: String,
    #[serde(rename = "type")]
    type_: String,
    pub(crate) topic: String,
    pub(crate) version: String,
    #[serde(default)]
    pub(crate) create_topic: bool,
    #[serde(default)]
    parameters: BTreeMap<String, ManagedConnectorParameterValue>,
    #[serde(default)]
    secrets: BTreeMap<String, SecretString>,
}

#[derive(Debug)]
pub struct ConnectorConfigLoadErr;

impl fmt::Display for ConnectorConfigLoadErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connector config load err")
    }
}

impl std::error::Error for ConnectorConfigLoadErr {}

impl ConnectorConfig {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<Self, ConnectorConfigLoadErr> {
        let mut file = File::open(path.into()).expect("Failed to load connector config");
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        let connector_config: Self = serde_yaml::from_str(&contents).unwrap();
        Ok(connector_config)
    }
}

impl From<ConnectorConfig> for ManagedConnectorSpec {
    fn from(config: ConnectorConfig) -> ManagedConnectorSpec {
        ManagedConnectorSpec {
            name: config.name,
            type_: config.type_,
            topic: config.topic,
            parameters: config.parameters,
            secrets: config.secrets,
            version: config.version.into(),
        }
    }
}
