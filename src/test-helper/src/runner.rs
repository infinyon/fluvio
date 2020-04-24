use std::time::Duration;

use log::debug;


 use k8_client::SharedK8Client;
 use k8_client::load_and_share;
 use k8_client::ClientError;
 use k8_obj_metadata::Spec;
 use k8_metadata::topic::TopicSpec;
use k8_metadata::spu::SpuSpec;
use k8_metadata::spg::SpuGroupSpec;
use k8_metadata_client::MetadataClient;
use flv_future_aio::timer::sleep;

use crate::TestOption;
use crate::environment::EnvironmentDriver;
use crate::environment::DevelopEnvDriver;
use crate::environment::K8EnvironmentDriver;
use crate::tests::ConsumeProducerRunner;
use crate::CommandUtil;
use crate::Target;
use crate::get_fluvio;

pub struct TestRunner {
    option: TestOption,
    env_driver: Box<dyn EnvironmentDriver>,
    client: SharedK8Client
}


impl TestRunner {

    pub fn new(option: TestOption) -> Self {

        let client = load_and_share().expect("client should not fail");
        let env_driver = if option.use_k8() {
            println!("using k8 environment driver");
            Box::new(K8EnvironmentDriver::new(option.clone(),client.clone())) as Box<dyn EnvironmentDriver>
        } else {
            println!("using development environment driver");
            Box::new(DevelopEnvDriver::new(option.clone(),client.clone())) as Box<dyn EnvironmentDriver>
        };

        Self {
            option,
            env_driver,
            client
        }
    }


    async fn delete_objects<S:Spec>(&self) -> Result<(), ClientError> 
    {

        let objects = self.client.retrieve_items_with_option::<S,_>("default",None).await?;

        // delete all existing objects
        if objects.items.len() > 0 {
            debug!("{} {} exists ",S::label(),objects.items.len());
            for object in objects.items {
                println!("deleting {}:{}",S::label(),object.metadata.name);
                self.client.delete_item::<S,_>(&object.metadata).await.expect("delete should work");
            }
        } 
        Ok(())
    }

    /// clean existing objects
    async fn delete_all(&self) -> Result<(), ClientError> {

        self.delete_objects::<TopicSpec>().await?;
        self.delete_objects::<SpuSpec>().await?;
        self.delete_objects::<SpuGroupSpec>().await
        
    }

    async fn find_target(&self) -> Target {
        self.env_driver.find_target().await.expect("svc not found")
    }


    /// cleanup before initialize
    pub async fn pre_init_cleanup(&mut self) {

        self.delete_all().await.expect("deleting existing object failed");
        debug!("waiting 1 second for object delete to sync");  
        sleep(Duration::from_millis(1000)).await;
        self.env_driver.pre_init_cleanup().await;
    
    }

    async fn setup_topic(&self,target: &Target) {
            
        println!("creating test topic: <test1>");
        
        get_fluvio()
            .expect("fluvio not founded")
            .arg("topic")
            .arg("create")
            .arg("--topic")
            .arg("test1")
            .arg("--partitions")
            .arg("1")
            .arg("--replication")
            .arg(self.option.replication().to_string())
            .target(target)
            .setup_client_tls(&self.env_driver.tls())
            .print()
            .wait_and_check();

        println!("topic created");
    }

    pub async fn setup(&mut self) -> Target {

        println!("launching sc");

        self.env_driver.launch_sc().await;

        let target = self.find_target().await;
      
        println!("launching spu group with size: {}",self.option.spu_count());
        self.env_driver.launch_spu_group(&target).await;

        sleep(Duration::from_secs(1)).await;

        // topic creation must be done after spu
        // we need to test what happens topic gets created before spu
        if self.option.init_topic() {
            self.setup_topic(&target).await;
            sleep(Duration::from_secs(1)).await;
        } else {
            println!("no topic initialized");
        }

        target

    }

    /// initialize profile
    fn init_profile(&self) {
        
        use std::env::temp_dir;
        use std::env::set_var;

        let profile_home = temp_dir().join(".fluvio");
        println!("using profile directory: {:?}",profile_home);
        set_var("FLV_PROFILE_PATH", profile_home.into_os_string());
    }

    /// run basic test
    pub async fn test(&mut self)  -> Result<(),() > {       
        
        if self.option.cleanup() {
            self.pre_init_cleanup().await;
        } else {
            println!("cleanup skipped");
        }

        if self.option.use_profile() {
            self.init_profile();
        }
        
    
        // either we found target thru setup or need to get target only
        let target = if self.option.setup() {
            self.setup().await
        } else {
            println!("setup skipped");
            self.find_target().await
        };
       
        sleep(Duration::from_secs(1)).await;

        // at this point, cluster is up, we need to ensure clean shutdown of cluster
        // no matter if produce or consumer test crashes

        let consumer_tester = ConsumeProducerRunner::new(self.option.clone());

        let tls = self.env_driver.tls().clone();
        if let Err(err) = std::panic::catch_unwind(move || {
            consumer_tester.run(tls,target);

        }) {
            eprintln!("producer/consumer crashes {:#?}",err);
            
        } 
        
        if self.option.terminate_after_consumer_test() {
            self.env_driver.terminate();
          
        } else {
            println!("server not shut down");
        }        

        Ok(())

    }

}


