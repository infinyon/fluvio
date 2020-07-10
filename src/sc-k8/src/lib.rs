mod init;
mod cli;
mod error;
pub mod operator;
pub mod k8_operations;
use error::ScK8Error;

use k8_client::metadata::TokenStreamResult as OrigTokenStreamResult;
pub type K8TokenStreamResult<S> = OrigTokenStreamResult<S, k8_client::ClientError>;
pub use init::main_k8_loop as main_loop;
pub use cli::ScOpt;