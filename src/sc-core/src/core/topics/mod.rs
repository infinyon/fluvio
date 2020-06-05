mod actions;
mod metadata;
mod reducer;
mod controller;

pub use self::actions::TopicActions;
pub use self::metadata::{TopicKV, TopicLocalStore};
pub use self::actions::TopicChangeRequest;
pub use self::reducer::TopicReducer;
pub use self::controller::TopicController;

use ::flv_metadata::topic::TopicSpec;
use crate::core::common::LSChange;
use crate::core::common::WSAction;

use crate::metadata::K8ClusterStateDispatcher;

pub type K8TopicChangeDispatcher<C> = K8ClusterStateDispatcher<TopicSpec, C>;
pub type TopicWSAction = WSAction<TopicSpec>;
pub type TopicLSChange = LSChange<TopicSpec>;
