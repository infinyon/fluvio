mod k8;
mod local;

pub use common::*;

mod common {
    use async_trait::async_trait;

    use crate::TestOption;

    /// Environment driver for test
    #[async_trait]
    pub trait EnvironmentDriver {
        /// remove cluster
        async fn remove_cluster(&self);

        /// install cluster
        async fn start_cluster(&self);
    }

    use super::k8::K8EnvironmentDriver;
    use super::local::LocalEnvDriver;

    pub fn create_driver(option: TestOption) -> Box<dyn EnvironmentDriver> {
        if option.use_k8_driver() {
            println!("using k8 environment driver");
            Box::new(K8EnvironmentDriver::new(option)) as Box<dyn EnvironmentDriver>
        } else {
            println!("using local environment driver");
            Box::new(LocalEnvDriver::new(option)) as Box<dyn EnvironmentDriver>
        }
    }
}
