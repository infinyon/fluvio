mod init;
mod cli;
mod error;
pub mod operator;
pub mod k8_operations;
use error::ScK8Error;

use std::sync::Arc;

use k8_client::K8Client;
use k8_client::ClientError;
use k8_metadata::client::TokenStreamResult as OrigTokenStreamResult;
use k8_config::K8Config;


pub type  SharedK8Client = Arc<K8Client>;
pub type  K8TokenStreamResult<S,P> = OrigTokenStreamResult<S,P,ClientError>;

pub fn new_shared(config: K8Config) -> SharedK8Client {
    let client= K8Client::new(config).expect("Error: K8 client failed to initialize!");
    Arc::new(client)
}

pub use init::main_k8_loop;


fn main() {
    
    flv_util::init_logger();

    main_k8_loop();
}
