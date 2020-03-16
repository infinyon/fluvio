

use std::time::Duration;
use std::process::Child;
use std::process::Command;

use log::debug;

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
        
    }

    async fn setup_topic(&self) {

        debug!("creating new topic cloud@fluvio.io");
        let topic = TopicSpec {
            partitions: Some(1),
            replication_factor: Some(self.option.replication as i32),
            ..Default::default()
        };
        let input = InputK8Obj::new(topic,InputObjectMeta {
            name: "test1".to_owned(),
            namespace: "default".to_owned(),
            ..Default::default()
        });

        self.client.create_item(input).await.expect("item created");
    }


    async fn launch_sc() -> Child {

        debug!("starting sc server");
        get_binary("sc-k8-server")
            .expect("unable to get sc-k8-server")
            .spawn()
            .expect("sc server failed to start")
        
    }

    /// launch spu
    async fn launch_spu(&self, spu_index: u16) -> Child {

        const BASE_PORT: u16 = 9005;
        const BASE_SPU: u16 = 5001;

        let spu_id = (BASE_SPU + spu_index) as i32;
        let public_port = BASE_PORT + spu_index * 2;
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
        get_binary("spu-server")
            .expect("unable to get spu-server")
            .arg("-i")
            .arg(format!("{}",spu_id))
            .arg("-p")
            .arg(format!("0.0.0.0:{}",public_port))
            .arg("-v")
            .arg(format!("0.0.0.0:{}",private_port))
            .arg("--reset")
            .spawn()
            .expect("spu server failed to start")
    }


    pub async fn setup(&self) -> Launcher {

        self.ensure_clean().await;
        self.setup_topic().await;

        let mut launcher = Launcher::new();
        launcher.add("sc",Self::launch_sc()).await;

        for i in 0..self.option.replication {
            launcher.add(&format!("spu-{}",i),self.launch_spu(i as u16)).await;
        }
        
        launcher
    }


}


