


use std::process::Command;
use std::time::Duration;

use async_trait::async_trait;

use flv_future_aio::timer::sleep;
use k8_client::SharedK8Client;
use k8_obj_core::pod::PodSpec;
use k8_obj_core::service::ServiceSpec;
use k8_obj_metadata::Spec;
use k8_obj_metadata::InputObjectMeta;
use k8_metadata_client::MetadataClient;
use k8_client::ClientError as K8ClientError;

use crate::TestOption;
use crate::get_fluvio;
use crate::CommandUtil;
use crate::Target;
use crate::TlsLoader;
use super::EnvironmentDriver;


pub struct K8EnvironmentDriver {
    client: SharedK8Client,
    option: TestOption,
    tls: TlsLoader
}

impl K8EnvironmentDriver {

    pub fn new(option: TestOption,client: SharedK8Client) -> Self {
        Self {
            option: option.clone(),
            client,
            tls: TlsLoader::new(option)
        }
    }

    // wait for i8 objects appear
    async fn wait_for_delete<S:Spec>(&self,input: &InputObjectMeta)  {

        for i in 0..100u16 {
            println!("checking to see if {} is deleted, count: {}",S::label(),i);
            match self.client.retrieve_item::<S,_>(input).await {
                Ok(_) => {
                    println!("sc {} still exists, sleeping 10 second",S::label());
                    sleep(Duration::from_millis(10000)).await;
                },
                Err(err) => match err {
                    K8ClientError::NotFound => {
                        println!("no sc {} found, can proceed to setup ",S::label());
                        return;
                    },
                    _ => assert!(false,format!("error: {}",err))
                }
            };
        }

        assert!(false,"waiting too many times, failing");

    }


    async fn wait_for_service_exist(&self,input: &InputObjectMeta) -> Option<String> {

        for i in 0..100u16 {
            println!("checking to see if svc exists, count: {}",i);
            match self.client.retrieve_item::<ServiceSpec,_>(input).await {
                Ok(svc) => {
                    // check if load balancer status exists
                    if let Some(addr) = svc.status.load_balancer.find_any_ip_or_host() {
                        println!("found svc load balancer addr: {}",addr);
                        return Some(format!("{}:9003",addr.to_owned()))
                    } else {
                        println!("svc exists but no load balancer exist yet, continue wait");
                        sleep(Duration::from_millis(3000)).await;
                    }
                },
                Err(err) => match err {
                    K8ClientError::NotFound => {
                        println!("no svc found, sleeping ");
                        sleep(Duration::from_millis(3000)).await;
                    },
                    _ => assert!(false,format!("error: {}",err))
                }
            };
        }

        None

    }

    fn set_profile(&self) {

        // need to disable profile, so always set tls
        let mut tls = self.tls.clone();
        tls.disable_profile();
        
        // set default profile
        get_fluvio()
            .expect("unable to get fluvio")
            .arg("profile")
            .arg("set-k8-profile")
            .setup_client_tls(&tls)
            .print()
            .wait_and_check();
    }
}
    

#[async_trait]
impl EnvironmentDriver for K8EnvironmentDriver {

    fn tls(&self) -> &TlsLoader {
        &self.tls
    }

    async fn pre_init_cleanup(&self) {

        println!("removing fluvio installation");
        Command::new("helm")
                     .arg("uninstall")
                     .arg("fluvio")
                     .print()
                     .wait();

        sleep(Duration::from_millis(500)).await;
        // check to see if sc-pod is terminated
        
        let sc_pod = InputObjectMeta::named("flv-sc", "default");
        self.wait_for_delete::<PodSpec>(&sc_pod).await;

        // delete pvc
        Command::new("kubectl")
            .arg("delete")
            .arg("persistentvolumeclaims")
            .arg("--all")
            .print()
            .wait_and_check();

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
                
        install.arg("install")
                .arg("--log")
                .arg("flv=debug,kf=debug");

        if self.option.tls() {
            install.arg("--tls");
        }

        install.print().wait_and_check();
    
        
    }

    async fn find_target(&self) -> Option<Target> {

        let sc_svc = InputObjectMeta::named("flv-sc-public", "default");
        self.wait_for_service_exist(&sc_svc).await.map(|addr| {
            if self.option.use_profile() {
                self.set_profile();
                Target::Profile
            } else {
                Target::direct(addr)
            }
            
        } )
        
        
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