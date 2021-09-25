pub mod k8;
pub mod local;

pub use common::*;

mod common {
    use async_trait::async_trait;

    use fluvio_cluster::{StartStatus, runtime::spu::SpuClusterManager};
    use serde::{Serialize, Deserialize};

    /// Environment driver for test
    #[async_trait]
    pub trait TestEnvironmentDriver {
        /// remove cluster
        async fn remove_cluster(&self);

        /// install cluster
        async fn start_cluster(&self) -> StartStatus;

        fn create_cluster_manager(&self) -> Box<dyn SpuClusterManager>;
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    pub enum EnvironmentType {
        K8,
        Local,
    }

    use crate::{setup::environment::k8::K8EnvironmentDriver, test_meta::environment::EnvironmentSetup};
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
