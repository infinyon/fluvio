use std::time::Duration;

use flv_future_aio::timer::sleep;

use crate::TestOption;
use crate::setup::Setup;
use crate::tests::test_consumer;

pub struct TestRunner(TestOption);

impl TestRunner {

    pub fn new(option: TestOption) -> Self {
        Self(option)
    }

    /// run basic test
    pub async fn basic_test(&self) -> Result<(),() > {       
        
        let setup = Setup::new(self.0.clone());

        if self.0.terminate {
            setup.ensure_clean().await;
            println!("cleanup done");
        } else {

            let launcher = setup.setup().await;

            // wait 1 seconds for sc and spu to spin up
            sleep(Duration::from_secs(1)).await;

            if self.0.test_consumer() {

                if let Err(err) = std::panic::catch_unwind(|| {

                    test_consumer(self.0.clone());

                }) {
                
                    launcher.terminate();
                    eprintln!("panic during test, shutting down servers first: {:#?}",err);
                    std::process::exit(-1);
                
                } else {
                    launcher.terminate();
                    println!("successful test");
                
                }    
            } else {
                println!("no test run, keeping servers around.  use -t to kill only later")
            }
        }

        Ok(())
    }
}


