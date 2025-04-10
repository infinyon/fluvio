use std::{any::Any, str::FromStr, time::Duration};

use clap::Parser;

use anyhow::{bail, Result};
use humantime::parse_duration;

use fluvio::consumer::OffsetManagementStrategy;
use fluvio_test_util::test_meta::{environment::EnvironmentSetup, TestCase, TestOption};

#[derive(Debug, Clone)]
pub struct ConsumerOffsetsTestCase {
    pub environment: EnvironmentSetup,
    pub option: ConsumerOffsetsTestOption,
}

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq)]
#[command(name = "Fluvio Producer Batch Test")]
pub struct ConsumerOffsetsTestOption {
    #[arg(long)]
    pub strategy: OffsetManagementStrategy,
    #[arg(long, value_parser=parse_duration)]
    pub offset_flush: Option<Duration>,
    #[arg(long, default_value = "beginning")]
    pub offset_start: Option<TestOffsetStart>,
}

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq)]
pub enum TestOffsetStart {
    #[default]
    Beginning,
    End,
}

impl FromStr for TestOffsetStart {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "beginning" => Ok(TestOffsetStart::Beginning),
            "end" => Ok(TestOffsetStart::End),
            _ => bail!("Invalid offset start value"),
        }
    }
}

impl TestOption for ConsumerOffsetsTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<TestCase> for ConsumerOffsetsTestCase {
    fn from(test_case: TestCase) -> Self {
        let consumer_offsets_option = test_case
            .option
            .as_any()
            .downcast_ref::<ConsumerOffsetsTestOption>()
            .expect("ConsumerOffsetsTestOption")
            .to_owned();
        ConsumerOffsetsTestCase {
            environment: test_case.environment,
            option: consumer_offsets_option,
        }
    }
}
