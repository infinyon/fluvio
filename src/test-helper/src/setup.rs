

use std::time::Duration;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::fs::remove_dir_all;
use std::fs::File;


use log::debug;
use log::error;

use flv_future_aio::timer::sleep;
use k8_client::SharedK8Client;
use k8_client::ClientError;
use k8_client::load_and_share;
use k8_metadata_client::MetadataClient;
use k8_metadata::topic::TopicSpec;
use k8_metadata::spu::SpuSpec;
use k8_metadata::spu::IngressPort;
use k8_metadata::spu::Endpoint;
use k8_metadata::spu::IngressAddr;
use k8_obj_metadata::InputK8Obj;
use k8_obj_metadata::InputObjectMeta;
use k8_obj_metadata::Spec;

use crate::TestOption;
use crate::launcher::Launcher;
use crate::get_binary;
use crate::command_exec;

pub struct Setup {
    client: SharedK8Client,
    option: TestOption
}

impl Setup {

    pub fn new(option: TestOption) -> Self {

        let client = load_and_share().expect("client should not fail");
        
        Self {
            client,
            option
        }
    }


    async fn delete_objects<S:Spec>(&self) -> Result<(), ClientError> 
    {

        let objects = self.client.retrieve_items_with_option::<S,_>("default",None).await?;

        // delete all existing objects
        if objects.items.len() > 0 {
            debug!("{} {} exists ",S::label(),objects.items.len());
            for object in objects.items {
                debug!("deleting {} topic: {}",S::label(),object.metadata.name);
                self.client.delete_item::<S,_>(&object.metadata).await.expect("delete should work");
            }
        } 
        Ok(())
    }

    /// ensure we have no topics
    async fn delete_all(&self) -> Result<(), ClientError> {

        self.delete_objects::<TopicSpec>().await?;
        self.delete_objects::<SpuSpec>().await
    }



    // clean up 
    pub async fn ensure_clean(&self) {
        println!("cleaning existing topics");
        self.delete_all().await.expect("not fail"); 
        debug!("waiting for topics to be deleted");  
        sleep(Duration::from_millis(1000)).await;
        // also kill existing spu and server

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

    async fn setup_topic(&self) {
        
        println!("creating test topic: <test1>");
        
        command_exec("fluvio","cli",
            | cmd | {

                cmd.arg("topic")
                    .arg("create")
                    .arg("--topic")
                    .arg("test1")
                    .arg("--partitions")
                    .arg("1")
                    .arg("--replication")
                    .arg(self.option.replication().to_string())
                    .arg("--sc")
                    .arg("localhost:9003");

                println!("topic cmd: {:#?}",cmd);

            });
           
        
    }


    async fn launch_sc(&self) -> Child {

        let outputs = File::create(format!("/tmp/sc.log")).expect("log file");
        let errors = outputs.try_clone().expect("error  file");

        debug!("starting sc server");
        let mut base = get_binary("sc-k8-server")
            .expect("unable to get sc-server");

        if self.option.tls() {
            
            let current_dir = std::env::current_dir().unwrap().join("tls").join("certs");
            base
                .arg("--tls")
                .arg("--server-cert")
                .arg(current_dir.join("server.crt").as_os_str())
                .arg("--server-key")
                .arg(current_dir.join("server.key").as_os_str())
                .arg("--bind-non-tls-public")
                .arg(format!("0.0.0.0:{}","9005"));
        }

        base.stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()
            .expect("sc server failed to start")
        
    }

    /// launch spu
    async fn launch_spu(&self, spu_index: u16) -> Child {

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
            let current_dir = std::env::current_dir().unwrap().join("tls").join("certs");
            binary
                .arg("--tls")
                .arg("--server-cert")
                .arg(current_dir.join("server.crt").as_os_str())
                .arg("--server-key")
                .arg(current_dir.join("server.key").as_os_str())
                .arg("--bind-non-tls-public")
                .arg(format!("0.0.0.0:{}",non_tls_port));
        }

        binary
            .arg("-i")
            .arg(format!("{}",spu_id))
            .arg("-p")
            .arg(format!("0.0.0.0:{}",public_port))
            .arg("-v")
            .arg(format!("0.0.0.0:{}",private_port))
            .stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()
            .expect("spu server failed to start")
    }


    pub async fn setup(&self) -> Launcher {


        let mut need_wait = false;

        if self.option.cleanup() {
            self.ensure_clean().await;
            need_wait = true;
        } else {
            println!("no cleanup done");
        }
       

        let mut launcher = Launcher::new();

        if self.option.setup() {

        
            launcher.add("sc",self.launch_sc()).await;

            let spu = self.option.spu_count();

            println!("spu count: {}",spu);
            
            for i in 0..spu {
                launcher.add(&format!("spu-{}",i),self.launch_spu(i)).await;
            }
            

            sleep(Duration::from_millis(200)).await;

            // topic creation must be done after spu
            // we need to test what happens topic gets created before spu
            if self.option.init_topic() {
                self.setup_topic().await;
            } else {
                println!("no topic initialized");
            }

            need_wait = true;
        }

        if need_wait {
            sleep(Duration::from_secs(1)).await;
        }
       
        
        launcher
    }


}


