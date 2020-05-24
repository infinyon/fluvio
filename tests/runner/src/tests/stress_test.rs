use crate::TestOption;

const MAX_COUNT: usize = 1000;

/// in this, we fetch first, then send outs 1000 message and fetch back
pub async fn stress_test(option: &TestOption) {

    println!("stress test with count: {}",MAX_COUNT);

    // first get batchs 

}