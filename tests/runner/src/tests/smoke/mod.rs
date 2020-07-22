mod consume;
mod produce;
mod message;

pub use runner::*;

use super::TestDriver;

mod runner {

    use std::time::Duration;
    use flv_future_aio::timer::sleep;

    use async_trait::async_trait;

    use crate::TestOption;
    use super::*;

    /// simple smoke test runner which tests
    pub struct SmokeTestRunner {
        option: TestOption,
    }

    impl SmokeTestRunner {
        pub fn new(option: TestOption) -> Self {
            Self { option }
        }

        async fn produce_and_consume_cli(&self) {
            if self.option.produce() {
                super::produce::produce_message(&self.option).await;
            } else {
                println!("produce skipped");
            }

            sleep(Duration::from_secs(1)).await;

            if self.option.test_consumer() {
                super::consume::validate_consume_message(&self.option).await;
            } else {
                println!("consume test skipped");
            }
        }
    }

    #[async_trait]
    impl TestDriver for SmokeTestRunner {
        /// run tester
        async fn run(&self) {
            //use futures::future::join_all;
            //use futures::future::join;

            //use flv_future_aio::task::run_block_on;

            //let mut listen_consumer_test = vec ![];

            println!("start testing...");

            /*
            if self.0.test_consumer() {
                for i in 0..self.0.replication() {
                    listen_consumer_test.push(consume::validate_consumer_listener(i,&self.0));
                }
            }
            */

            /*
            run_block_on(
                join(
                    self.produce_and_consume_cli(&target),
                    join_all(listen_consumer_test)
                ));
            */
            self.produce_and_consume_cli().await;
        }
    }
}
