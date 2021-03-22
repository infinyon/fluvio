pub mod k8;
pub mod local;

pub use common::*;

mod common {
    use async_trait::async_trait;

    use fluvio_cluster::StartStatus;
    use serde::{Serialize, Deserialize};

    /// Environment driver for test
    #[async_trait]
    pub trait TestEnvironmentDriver {
        /// remove cluster
        async fn remove_cluster(&self);

        /// install cluster
        async fn start_cluster(&self) -> StartStatus;
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum EnvironmentType {
        K8,
        Local,
    }

    use crate::{setup::environment::k8::K8EnvironmentDriver, test_meta::EnvironmentSetup};
    use crate::setup::environment::local::LocalEnvDriver;

    pub fn create_driver(option: EnvironmentSetup) -> Box<dyn TestEnvironmentDriver> {
        if option.local {
            //println!("using local environment driver");
            Box::new(LocalEnvDriver::new(option)) as Box<dyn TestEnvironmentDriver>
        } else {
            //println!("using k8 environment driver");
            Box::new(K8EnvironmentDriver::new(option)) as Box<dyn TestEnvironmentDriver>
        }
    }
}
