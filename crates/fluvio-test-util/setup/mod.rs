pub mod environment;

pub use cluster::*;

mod cluster {

    use std::time::Duration;

    use anyhow::Result;

    use fluvio_future::timer::sleep;
    use fluvio::{Fluvio, FluvioConfig};

    use crate::tls::load_tls;
    use crate::test_meta::environment::{EnvironmentSetup, EnvDetail};

    use super::environment::{TestEnvironmentDriver, create_driver};

    #[derive(Clone)]
    pub struct TestCluster {
        option: EnvironmentSetup,
        env_driver: TestEnvironmentDriver,
    }

    impl TestCluster {
        pub fn new(option: EnvironmentSetup) -> Self {
            // Can we condense the interface to the environment?
            let env_driver = create_driver(option.clone());

            Self { option, env_driver }
        }

        pub fn env_driver(&self) -> &TestEnvironmentDriver {
            &self.env_driver
        }

        /// cleanup before initialize
        pub async fn remove_cluster(&mut self) {
            // make sure delete all
            println!("deleting cluster");

            self.env_driver.remove_cluster().await;
        }

        pub async fn start(&mut self) -> Result<Fluvio> {
            if self.option.remove_cluster_before() {
                self.remove_cluster().await;
            } else {
                println!("remove cluster skipped");
            }

            println!("installing cluster");

            let cluster_status = self.env_driver.start_cluster().await;

            sleep(Duration::from_millis(2000)).await;

            let fluvio_config = if self.option.tls {
                let (client, _server) = load_tls(&self.option.tls_user);
                FluvioConfig::new(cluster_status.address()).with_tls(client)
            } else {
                FluvioConfig::new(cluster_status.address())
            };

            let fluvio = Fluvio::connect_with_config(&fluvio_config).await?;

            Ok(fluvio)
        }
    }
}
