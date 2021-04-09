mod actions;
mod reducer;
mod controller;
mod policy;

pub use self::actions::*;
pub use self::controller::*;
pub use self::policy::*;
pub use common::*;

mod common {

    use ::fluvio_controlplane_metadata::topic::TopicSpec;
    use crate::stores::actions::WSAction;

    #[allow(clippy::upper_case_acronyms)]
    pub type TopicWSAction = WSAction<TopicSpec>;
}
