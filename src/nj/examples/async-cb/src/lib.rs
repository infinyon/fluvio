
use std::time::Duration;

use flv_future_core::sleep;
use nj::derive::node_bindgen;


#[node_bindgen]
async fn hello<F: Fn(f64,String)>( cb: F) {
        
    println!("sleeping");
    sleep(Duration::from_secs(1)).await;
    println!("woke from time");

    cb(10.0,"hello world".to_string());

}



