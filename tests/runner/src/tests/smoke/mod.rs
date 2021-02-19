mod consume;
mod produce;
mod message;
mod concurrent;

pub use runner::*;

use super::TestDriver;

mod runner {

    use async_trait::async_trait;

    use crate::TestOption;
    use super::*;
    use crate::tests::smoke::concurrent::test_concurrent_consume_produce;

    /// simple smoke test runner which tests
    pub struct SmokeTestRunner {
        option: TestOption,
    }

    impl SmokeTestRunner {
        pub fn new(option: TestOption) -> Self {
            Self { option }
        }

        async fn produce_and_consume_cli(&self) {
            // let start_offsets = super::produce::produce_message(&self.option).await;
            // super::consume::validate_consume_message(&self.option, start_offsets).await;

            test_concurrent_consume_produce().await;
        }
    }

    #[async_trait]
    impl TestDriver for SmokeTestRunner {
        /// run tester
        async fn run(&self) {
            println!("start testing...");

            self.produce_and_consume_cli().await;
        }
    }
}
