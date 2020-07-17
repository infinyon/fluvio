//!
//! # SPU Actions
//!
//!

use flv_types::SpuId;

/// action for SPU controller to take
#[derive(Debug, PartialEq, Clone)]
pub struct SpuAction {
    pub id: SpuId,
    pub status: bool,
}

impl SpuAction {
    pub fn up(id: SpuId) -> Self {
        Self { id, status: true }
    }

    pub fn down(id: SpuId) -> Self {
        Self { id, status: false }
    }
}
