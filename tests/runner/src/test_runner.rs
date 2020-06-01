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
        Self { option }
    }


    /*
    fn wait_for_topic(&self) {


         
        // wait until topic is provisioned
        // topic describe is not correct since it doesn't specify partition
        for _ in 0..100u16 {
            let output = get_fluvio()
                .expect("fluvio not founded")
                .log(self.option.log.as_ref())
                .arg("topic")
                .arg("describe")
                .arg("--topic")
                .arg(topic_name)
                .print()
                .output()
                .expect("topic describe");

            std::io::stderr().write_all(&output.stderr).unwrap();
            if output.status.success() {
                // check if output contains provisioned word, may do json
                let msg = String::from_utf8(output.stdout).expect("output");
                println!("output: {}", msg);
                if msg.contains("provisioned") {
                    println!("topic {} provisioned", topic_name);
                    return;
                } else {
                    println!("topic {} not provisioned, waiting 2 seconds", topic_name);
                }
            } else {
                println!("topic: {} not provisioned, waiting 2 second", topic_name);
            }
            sleep(Duration::from_secs(2)).await
        }

        assert!(false, "unable to provision topic: {}", topic_name);
        

    }
    */

    async fn setup_topic(&self) {
        
        // wait until SPU come online
        sleep(Duration::from_secs(2)).await;

        let topic_name = &self.option.topic_name;

        println!("creating test topic: <{}>", topic_name);

        get_fluvio()
            .expect("fluvio not founded")
            .arg("topic")
            .arg("create")
            .arg(topic_name)
            .arg("--replication")
            .arg(self.option.replication().to_string())
            .print()
            .inherit();

        println!("topic created");

        // wait until topic is created, this is hack for now until we have correct
        // implementation of find topic
        sleep(Duration::from_secs(5)).await
       
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

        // sleep(Duration::from_secs(3)).await;
        } else {
            println!("no topic initialized");
        }

        let test_driver = create_test_driver(self.option.clone());

        test_driver.run().await;
    }
}
