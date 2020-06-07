use std::time::Duration;

use flv_future_aio::timer::sleep;

use crate::cli::TestOption;
use crate::environment::EnvironmentDriver;

pub struct Setup {
    option: TestOption,
    env_driver: Box<dyn EnvironmentDriver>,
}

impl Setup {
    pub fn new(option: TestOption) -> Self {
        use crate::environment::create_driver;

        let env_driver = create_driver(option.clone());

        Self { option, env_driver }
    }

    /// cleanup before initialize
    async fn remove_cluster(&mut self) {
        // make sure delete all

        self.env_driver.remove_cluster().await;
    }

    /// set up
    pub async fn setup(&mut self) {
        if self.option.remove_cluster_before() {
            self.remove_cluster().await;
        } else {
            println!("remove cluster skipped");
        }

        println!("installing cluster");

        self.env_driver.install_cluster().await;

        sleep(Duration::from_millis(100)).await;
    }
}
