
use std::time::Duration;
use flv_future_aio::timer::sleep;

use utils::bin::get_fluvio;

use crate::cli::TestOption;
use crate::util::CommandUtil;

/// run test
pub struct TestRunner {
    option: TestOption,
}

impl TestRunner {

    pub fn new(option: TestOption) -> Self {

        Self {
            option,
        }

    }

    async fn setup_topic(&self) {

       
            
        println!("creating test topic: <test1>");
        
        get_fluvio()
            .expect("fluvio not founded")
            .arg("topic")
            .arg("create")
            .arg("test1")
            .arg("--replication")
            .arg(self.option.replication().to_string())
         //   .setup_client_tls(&self.env_driver.tls())
            .print()
            .wait_and_check();

        println!("topic created");
    }

    /// main entry point
    pub async fn run_test(&self) {       
        
        use crate::tests::create_test_driver;

        // at this point, cluster is up, we need to ensure clean shutdown of cluster
        // no matter if produce or consumer test crashes

        // topic creation must be done after spu
        // we need to test what happens topic gets created before spu
        if self.option.init_topic() {
            self.setup_topic().await;
            println!("wait til topic is created");
            sleep(Duration::from_secs(5)).await;
        } else {
            println!("no topic initialized");
        }

        let test_driver = create_test_driver(self.option.clone());
       
        if let Err(err) = std::panic::catch_unwind(move || {
            test_driver.run();

        }) {
            eprintln!("producer/consumer crashes {:#?}",err);
            
        } 
        
    }


}