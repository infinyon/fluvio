pub mod update_spu;
pub mod update_replica;
pub mod register_spu;
pub mod update_lrs;
pub mod remove;
pub mod update_smartmodule;
pub mod update_derivedstream;

mod request;
pub use self::request::ControlPlaneRequest;
