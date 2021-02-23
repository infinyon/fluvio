pub mod environment;
use environment::TestEnvironmentDriver;

use std::time::Duration;

use fluvio_future::timer::sleep;

use crate::{test_meta::TestOption, tls::load_tls};

use fluvio::{Fluvio, FluvioConfig, FluvioError};

pub struct TestCluster {
    option: TestOption,
    env_driver: Box<dyn TestEnvironmentDriver>,
}

impl TestCluster {
    pub fn new(option: TestOption) -> Self {
        use environment::create_driver;

        // Can we condense the interface to the environment?
        let env_driver = create_driver(option.clone());

        Self { option, env_driver }
    }

    /// cleanup before initialize
    pub async fn remove_cluster(&mut self) {
        // make sure delete all
        println!("deleting cluster");

        self.env_driver.remove_cluster().await;
    }

    pub async fn start(&mut self) -> Result<Fluvio, FluvioError> {
        if self.option.remove_cluster_before() {
            self.remove_cluster().await;
        } else {
            println!("remove cluster skipped");
        }

        println!("installing cluster");

        let cluster_status = self.env_driver.start_cluster().await;

        sleep(Duration::from_millis(2000)).await;

        let fluvio_config = if self.option.tls() {
            let (client, _server) = load_tls(&self.option.tls_user);
            FluvioConfig::new(cluster_status.address()).with_tls(client)
        } else {
            FluvioConfig::new(cluster_status.address())
        };

        let fluvio = Fluvio::connect_with_config(&fluvio_config).await?;

        Ok(fluvio)
    }
}
