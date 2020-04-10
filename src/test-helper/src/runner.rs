
use crate::TestOption;
use crate::setup::Setup;
use crate::tests::test_consumer;
use crate::tests::produce_message_with_cli;
pub struct TestRunner(TestOption);

impl TestRunner {

    pub fn new(option: TestOption) -> Self {
        Self(option)
    }

    /// run basic test
    pub async fn basic_test(&self) -> Result<(),() > {       
        
        let setup = Setup::new(self.0.clone());

        let launcher = setup.setup().await;
       
    
        if self.0.produce() {
            produce_message_with_cli();
        }



        if self.0.test_consumer() {

            if let Err(err) = std::panic::catch_unwind(|| {

                test_consumer(self.0.clone());

            }) {
            
                eprintln!("test failed during consumer test {:#?}",err);
                if self.0.terminate_after_consumer_test() {
                    launcher.terminate();
                  
                } else {
                    eprintln!("server not shut down");
                }
                assert!(false);
                
            
            } else {

                println!("successful test");
                if self.0.terminate_after_consumer_test() {
                    launcher.terminate();
                } else {
                    println!("server not shutdown ")
                }
                assert!(true);
                
            }    
        } 

        Ok(())
    }
}


