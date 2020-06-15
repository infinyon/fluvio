mod sc_api;
mod spu_api;
mod requests;

pub use self::sc_api::InternalScKey;
pub use self::sc_api::InternalScRequest;
pub use self::spu_api::InternalSpuApi;
pub use self::spu_api::InternalSpuRequest;

pub use self::requests::update_spu::*;
pub use self::requests::update_replica::*;
pub use self::requests::register_spu::*;
pub use self::requests::update_lrs::*;
pub use self::requests::update_all::*;

use kf_protocol::api::RequestMessage;

pub type UpdateSpuRequestMessage = RequestMessage<UpdateSpuRequest>;

pub mod messages {
    pub use flv_metadata::api::*;
}
