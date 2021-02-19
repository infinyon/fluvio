mod smoke;
mod stress;

use async_trait::async_trait;
pub use smoke::SmokeTestRunner;
use crate::TestOption;

#[async_trait]
pub trait TestDriver {
    /// run tester
    async fn run(&self);
}

pub fn create_test_driver(option: TestOption) -> Box<dyn TestDriver> {
    Box::new(SmokeTestRunner::new(option))
}
