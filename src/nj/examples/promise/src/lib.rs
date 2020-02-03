use std::time::Duration;

use flv_future_core::sleep;
use nj::derive::node_bindgen;



#[node_bindgen]
async fn hello(arg: f64) -> f64 {
    println!("sleeping");
    sleep(Duration::from_secs(1)).await;
    println!("woke and adding 10.0");
    arg + 10.0
}
