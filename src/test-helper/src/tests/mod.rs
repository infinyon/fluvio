mod consume;
mod produce;


use crate::TestOption;
use crate::Target;
use crate::TlsLoader;

pub struct ConsumeProducerRunner(TestOption);


impl ConsumeProducerRunner {

    pub fn new(option: TestOption) -> Self {
        Self(option)
    }


    /// run tester
    pub fn run(&self,tls: TlsLoader,target: Target) {

        //use futures::future::join_all;
        //use futures::future::join;

        //use flv_future_aio::task::run_block_on;

        //let mut listen_consumer_test = vec ![];

        println!("start testing...");

        /*
        if self.0.test_consumer() {
            for i in 0..self.0.replication() {
                listen_consumer_test.push(consume::validate_consumer_listener(i,&self.0));
            }    
        }
        */

        /*
        run_block_on(
            join(
                self.produce_and_consume_cli(&target),
                join_all(listen_consumer_test)
            ));
        */
        self.produce_and_consume_cli(&tls,&target);
    }
        

    fn produce_and_consume_cli(&self,tls: &TlsLoader,target: &Target)  {

        // sleep 100 ms to allow listener to start earlier
        if self.0.produce() {
            produce::produce_message_with_cli(tls,target);
        } else {
            println!("produce skipped");
        }
       
        
        if self.0.test_consumer() {
            consume::validate_consume_message_cli(tls,target);
        } else {
            println!("consume test skipped");
        }
        
        
    }

}