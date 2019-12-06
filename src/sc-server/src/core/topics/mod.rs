mod actions;
mod metadata;
mod reducer;
mod controller;

pub use self::actions::TopicActions;
pub use self::metadata::{TopicKV, TopicLocalStore};
pub use self::actions::TopicChangeRequest;
pub use self::reducer::TopicReducer;
pub use self::controller::TopicController;

use ::metadata::topic::TopicSpec;
use crate::core::common::LSChange;
use crate::core::common::WSAction;


use crate::k8::K8ClusterStateDispatcher;


pub type K8TopicChangeDispatcher = K8ClusterStateDispatcher<TopicSpec>;
pub type TopicWSAction = WSAction<TopicSpec>;
pub type TopicLSChange = LSChange<TopicSpec>;


