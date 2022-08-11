pub mod k8;
pub mod local;

pub use common::*;

mod common {
    use async_trait::async_trait;
    use serde::{Serialize, Deserialize};

    use fluvio_cluster::{StartStatus, runtime::spu::SpuClusterManager};

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub enum EnvironmentType {
        K8,
        Local,
    }

    #[async_trait]
    pub trait EnvironmentDriver {
        /// remove cluster
        async fn remove_cluster(&self);

        /// install cluster
        async fn start_cluster(&self) -> StartStatus;

        fn create_cluster_manager(&self) -> Box<dyn SpuClusterManager>;
    }

    #[derive(Clone)]
    pub enum TestEnvironmentDriver {
        K8(Box<K8EnvironmentDriver>),
        Local(Box<LocalEnvDriver>),
    }

    impl TestEnvironmentDriver {
        /// remove cluster
        pub async fn remove_cluster(&self) {
            match self {
                Self::K8(k8) => k8.remove_cluster().await,
                Self::Local(local) => local.remove_cluster().await,
            }
        }

        /// install cluster
        pub async fn start_cluster(&self) -> StartStatus {
            match self {
                Self::K8(k8) => k8.start_cluster().await,
                Self::Local(local) => local.start_cluster().await,
            }
        }

        pub fn create_cluster_manager(&self) -> Box<dyn SpuClusterManager> {
            match self {
                Self::K8(k8) => k8.create_cluster_manager(),
                Self::Local(local) => local.create_cluster_manager(),
            }
        }
    }

    use crate::{setup::environment::k8::K8EnvironmentDriver, test_meta::environment::EnvironmentSetup};
    use crate::setup::environment::local::LocalEnvDriver;

    pub fn create_driver(option: EnvironmentSetup) -> TestEnvironmentDriver {
        if option.local {
            //println!("using local environment driver");
            TestEnvironmentDriver::Local(Box::new(LocalEnvDriver::new(option)))
        } else {
            //println!("using k8 environment driver");
            TestEnvironmentDriver::K8(Box::new(K8EnvironmentDriver::new(option)))
        }
    }
}
