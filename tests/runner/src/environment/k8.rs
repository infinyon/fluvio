


use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;

use flv_future_aio::timer::sleep;
use k8_client::SharedK8Client;


use crate::TestOption;
use crate::get_fluvio;
use crate::CommandUtil;
use crate::Target;
use crate::TlsLoader;
use super::EnvironmentDriver;


pub struct K8EnvironmentDriver {
    option: TestOption,
    tls: TlsLoader
}

impl K8EnvironmentDriver {

    pub fn new(option: TestOption,_client: SharedK8Client) -> Self {
        Self {
            option: option.clone(),
            tls: TlsLoader::new(option)
        }
    }

        
}
    

#[async_trait]
impl EnvironmentDriver for K8EnvironmentDriver {

    fn tls(&self) -> &TlsLoader {
        &self.tls
    }

    async fn pre_init_cleanup(&self) {

    

        // delete secrets
        Command::new("kubectl")
            .arg("delete")
            .arg("secret")
            .arg("fluvio-ca")
            .arg("--ignore-not-found=true")
            .print()
            .wait_and_check();

        Command::new("kubectl")
            .arg("delete")
            .arg("secret")
            .arg("fluvio-tls")
            .arg("--ignore-not-found=true")
            .print()
            .wait_and_check();

    }

    async fn launch_sc(&mut self) {

        if self.option.tls() {
            println!("creating secrets");
            Command::new("kubectl")
                .arg("create")
                .arg("secret")
                .arg("generic")
                .arg("fluvio-ca")
                .arg("--from-file")
                .arg("tls/certs/ca.crt")
                .print()
                .wait_and_check();

            Command::new("kubectl")
                .arg("create")
                .arg("secret")
                .arg("tls")
                .arg("fluvio-tls")
                .arg("--cert")
                .arg("tls/certs/server.crt")
                .arg("--key")
                .arg("tls/certs/server.key")
                .print()
                .wait_and_check();

        }

        println!("installing fluvio");
        let mut install = get_fluvio()
                .expect("unable to get fluvio");
                
        install
            .arg("cluster")
            .arg("install")
            .arg("--spu")
            .arg("0")
            .arg("--log")
            .arg(&self.option.log);

        if self.option.tls() {
            install.arg("--tls");
        }

        if self.option.develop_mode() {
            install.arg("--develop");
        }

        install.print().wait_and_check();
    
        
    }

    async fn find_target(&self) -> Option<Target> {

        Some(Target::Profile)
        
    }

    async fn launch_spu_group(&mut self,target: &Target) {

        let spu = self.option.spu_count();
        get_fluvio()
                .expect("unable to get fluvio")
                .arg("spu-group")
                .arg("create")
                .arg("--name")
                .arg("main")
                .arg("--replicas")
                .arg(spu.to_string())
                .target(target)
                .setup_client_tls(&self.tls)
                .print()
                .wait_and_check();
      
        sleep(Duration::from_millis(1000)).await;
    }

    fn terminate(&mut self)  {

    }
}