
mod k8;

pub use common::*;
pub use develop::*;
pub use k8::*;

mod common {

    use async_trait::async_trait;

    use crate::Target;
    use crate::TlsLoader;

    /// Environment driver for test
    #[async_trait]
    pub trait EnvironmentDriver {

        fn tls(&self) -> &TlsLoader;

        /// any cleanup before init/setup
        async fn pre_init_cleanup(&self);


        /// start sc and return target sc
        async fn launch_sc(&mut self);

        /// find target to cluster, this will wait until, cluster stabilizes
        async fn find_target(&self) -> Option<Target>;

        /// start spu group
        async fn launch_spu_group(&mut self,target: &Target);

        /// clean up
        fn terminate(&mut self);
    }
}


mod develop {

    use std::time::Duration;
    use std::process::Child;
    use std::process::Command;
    use std::process::Stdio;
    use std::fs::remove_dir_all;
    use std::fs::File;

    use log::debug;
    use log::error;
    use async_trait::async_trait;

    use k8_client::SharedK8Client;
    use flv_future_aio::timer::sleep;
    use k8_obj_metadata::InputK8Obj;
    use k8_metadata::spu::SpuSpec;
    use k8_obj_metadata::InputObjectMeta;
    use k8_metadata::spu::IngressPort;
    use k8_metadata::spu::Endpoint;
    use k8_metadata::spu::IngressAddr;
    use k8_metadata_client::MetadataClient;

    use crate::TestOption;
    use crate::get_binary;
    use crate::CommandUtil;
    use crate::Target;
    use crate::TlsLoader;
    use crate::get_fluvio;

    use super::EnvironmentDriver;

    /// Develop environment where we run sc/spu on native executable
    pub struct DevelopEnvDriver {
        option: TestOption,
        client: SharedK8Client,
        servers: Vec<Child>,
        tls: TlsLoader
    }
        

    impl DevelopEnvDriver {

        pub fn new(option: TestOption,client: SharedK8Client) -> Self {
            Self {
                option: option.clone(),
                client,
                servers: vec![],
                tls: TlsLoader::new(option)
            }
        }

        fn add(&mut self, name: &str, child: Child)
        {
            println!("server: {}:{} is being tracked",name,child.id());
            self.servers.push(child);
        }

        async fn launch_spu(&mut self, spu_index: u16)  {

            const BASE_PORT: u16 = 9010;
            const BASE_SPU: u16 = 5001;

            let spu_id = (BASE_SPU + spu_index) as i32;
            let public_port = BASE_PORT + spu_index * 10;
            let private_port = public_port + 1;

            let spu_spec = SpuSpec {
                spu_id,
                public_endpoint: IngressPort {
                    port: public_port,
                    ingress: vec![IngressAddr {
                        hostname: Some("localhost".to_owned()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                private_endpoint: Endpoint {
                    port: private_port,
                    host: "localhost".to_owned(),
                    ..Default::default()
                },
                ..Default::default()
                
            };

            let input = InputK8Obj::new(spu_spec,InputObjectMeta {
                name: format!("custom-spu-{}",spu_id),
                namespace: "default".to_owned(),
                ..Default::default()
            });

            self.client.create_item(input).await.expect("item created");


            // sleep 1 seconds for sc to connect
            sleep(Duration::from_millis(300)).await;

            let outputs = File::create(format!("/tmp/spu_log_{}.log",spu_id)).expect("log file");
            let errors = outputs.try_clone().expect("error  file");

            let mut binary = get_binary("spu-server")
                .expect("unable to get spu-server");
                

            if self.option.tls() {
            
                let non_tls_port = (private_port + 1).to_string();
                binary
                    .setup_server_tls(&self.tls)
                    .arg("--bind-non-tls-public")
                    .arg(format!("0.0.0.0:{}",non_tls_port));
            }

            let cmd = binary
                .arg("-i")
                .arg(format!("{}",spu_id))
                .arg("-p")
                .arg(format!("0.0.0.0:{}",public_port))
                .arg("-v")
                .arg(format!("0.0.0.0:{}",private_port))
                .print();

            println!("SPU<{}> cmd: {:#?}",spu_index,cmd);

            let spu_server = cmd
                .stdout(Stdio::from(outputs))
                .stderr(Stdio::from(errors))
                .spawn()
                .expect("spu server failed to start");

            self.add(&format!("spu-{}",spu_index),spu_server);
        }

        /// set profile if required
        fn set_profile(&self) {

            // need to disable profile, so always set tls
            let mut tls = self.tls.clone();
            tls.disable_profile();
            
            get_fluvio()
                .expect("unable to get fluvio")
                .arg("profile")
                .arg("set-local-profile")
                .setup_client_tls(&tls)
                .print()
                .wait_and_check();
        }

    }

    #[async_trait]
    impl EnvironmentDriver for DevelopEnvDriver {

        fn tls(&self) -> &TlsLoader {
            &self.tls
        }

        async fn pre_init_cleanup(&self) {
           
            println!("ensuring existing servers are terminated");
            Command::new("pkill")
                .arg("sc-k8-server")
                .arg("spu-server")
                .output()
                .expect("failed to execute process");


            // delete fluvio file
            debug!("remove fluvio directory");
            if let Err(err) = remove_dir_all("/tmp/fluvio") {
                error!("fluvio dir can't be removed: {}",err);
            }
            
        }


        async fn launch_sc(&mut self)  {

            let outputs = File::create(format!("/tmp/flv_sc.log")).expect("log file");
            let errors = outputs.try_clone().expect("error  file");

            debug!("starting sc server");
            let mut base = get_binary("sc-k8-server")
                .expect("unable to get sc-server");

            if self.option.tls() {
                
                println!("starting SC with TLS options");
                base
                    .setup_server_tls(&self.tls)
                    .arg("--bind-non-tls-public")
                    .arg(format!("0.0.0.0:{}","9005"));
            };

            base.print();

            let sc_server = base.stdout(Stdio::from(outputs))
                .stderr(Stdio::from(errors))
                .spawn()
                .expect("sc server failed to start");

            self.add("sc",sc_server);
        }

        

        async fn find_target(&self) -> Option<Target> {
            if self.option.use_profile() {
                self.set_profile();
                Some(Target::Profile)
            } else {
                Some(Target::direct("localhost:9003".to_owned()))
            }
        }   

        async fn launch_spu_group(&mut self,_target: &Target) {

            let spu = self.option.spu_count();

            for i in 0..spu {
                println!("launching spu<{}> out of {}",i,spu);
                self.launch_spu(i).await;
            }
                
            sleep(Duration::from_millis(500)).await;

        }

        fn terminate(&mut self)  {

            for server in &mut self.servers {
                println!("shutting down server: {}",server.id());
                if let Err(err) = server.kill() {
                    eprintln!("fail to terminate: {}",err);
                }
            }
    
        }
        

    }

}