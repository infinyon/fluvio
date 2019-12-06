mod actions;
mod metadata;
mod controller;
mod reducer;


pub use self::actions::PartitionActions;
pub use self::actions::PartitionChangeRequest;
pub use self::metadata::{PartitionKV, PartitionLocalStore};
pub use self::controller::PartitionController;

use std::sync::Arc;
use ::metadata::partition::PartitionSpec;
use reducer::PartitionReducer;
use crate::core::common::WSAction;
use crate::core::common::LSChange;
use crate::k8::K8ClusterStateDispatcher;

pub type K8PartitionChangeDispatcher = K8ClusterStateDispatcher<PartitionSpec>;
pub type PartitionWSAction = WSAction<PartitionSpec>;
pub type SharedPartitionStore = Arc<PartitionLocalStore>;
pub type PartitionLSChange = LSChange<PartitionSpec>;

