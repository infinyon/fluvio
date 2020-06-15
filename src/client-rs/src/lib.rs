mod error;
mod spu_old;
mod replica;
//mod sc;
pub mod client;

pub mod metadata;
pub mod profile;
pub mod query_params;

pub use error::ClientError;
pub use spu_old::SpuReplicaLeader;
pub use replica::*;

pub const MAX_FETCH_BYTES: u32 = 1000000;
