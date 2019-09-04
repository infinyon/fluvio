mod actions;
mod metadata;
mod reducer;
mod controller;

pub use self::actions::SpuActions;
pub use self::actions::SpuChangeRequest;
pub use self::metadata::{SpuKV, SpuLocalStore};
pub use self::reducer::SpuReducer;
pub use self::controller::SpuController;


use std::sync::Arc;
use ::metadata::spu::SpuSpec;
use crate::core::common::LSChange;

use crate::k8::K8ClusterStateDispatcher;


pub type K8SpuChangeDispatcher = K8ClusterStateDispatcher<SpuSpec>;
pub type SharedSpuLocalStore = Arc<SpuLocalStore>;
pub type SpuLSChange = LSChange<SpuSpec>;