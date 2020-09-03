mod init;
mod cli;
mod error;
mod service;
pub mod operator;
use error::ScK8Error;

use k8_client::metadata::TokenStreamResult as OrigTokenStreamResult;
pub type K8TokenStreamResult<S> = OrigTokenStreamResult<S, k8_client::ClientError>;
pub use init::main_k8_loop as main_loop;
pub use cli::ScOpt;




use flv_sc_core::dispatcher;

use flv_sc_core::stores;

use flv_sc_core::core;

use crate::dispatcher::k8;