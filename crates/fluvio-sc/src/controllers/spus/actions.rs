//!
//! # SPU Actions
//!
//!

use fluvio_types::SpuId;

/// action for SPU controller to take
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct SpuAction {
    pub id: SpuId,
    pub status: bool,
}
