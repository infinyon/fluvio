
mod k8;
mod local;

pub use common::*;

mod common {

    use std::process::Command;
    
    use async_trait::async_trait;

    use crate::TestOption;

    
    /// Environment driver for test
    #[async_trait]
    pub trait EnvironmentDriver {


        /// remove cluster
        async fn remove_cluster(&self);

        /// install cluster
        async fn install_cluster(&self);


        fn set_tls(&self,option: &TestOption,cmd: &mut Command) {

            use crate::tls::Cert;

            if option.tls() {

                let client_dir = Cert::load_client();
    
                cmd.arg("--tls")
                    .arg("--domain")
                    .arg("fluvio.local")
                    .arg("--ca-cert")
                    .arg(client_dir.ca.as_os_str())
                    .arg("--client-cert")
                    .arg(client_dir.cert.as_os_str())
                    .arg("--client-key")
                    .arg(client_dir.key.as_os_str());
    
                let server_dir = Cert::load_server();
                cmd
                    .arg("--server-key")
                    .arg(server_dir.key.as_os_str())
                    .arg("--server-cert")
                    .arg(server_dir.cert.as_os_str());
    
            }
        }

    }


    use super::k8::K8EnvironmentDriver;
    use super::local::LocalEnvDriver;


    pub fn create_driver(option: TestOption) ->Box<dyn EnvironmentDriver> {

        if option.use_k8_driver() {
            println!("using k8 environment driver");
            Box::new(K8EnvironmentDriver::new(option.clone())) as Box<dyn EnvironmentDriver>
        } else {
            println!("using local environment driver");
            Box::new(LocalEnvDriver::new(option.clone())) as Box<dyn EnvironmentDriver>
        }
    }
    

}