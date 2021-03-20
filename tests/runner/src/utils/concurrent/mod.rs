use std::any::Any;
use crate::test_meta::{EnvironmentSetup, TestOption, TestCase};
use structopt::StructOpt;

#[derive(Debug, Clone)]
pub struct ConcurrentTestCase {
    pub environment: EnvironmentSetup,
    pub option: ConcurrentTestOption,
}

impl From<TestCase> for ConcurrentTestCase {
    fn from(test_case: TestCase) -> Self {
        ConcurrentTestCase {
            environment: test_case.environment,
            option: ConcurrentTestOption {},
        }
    }
}

#[derive(Debug, Clone, StructOpt, Default)]
pub struct ConcurrentTestOption {}

impl TestOption for ConcurrentTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
