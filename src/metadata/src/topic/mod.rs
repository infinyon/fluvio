mod spec;
mod status;
pub mod store;

pub use self::spec::*;
pub use self::status::*;


pub const PENDING_REASON: &'static str = "waiting for live spus";



#[cfg(feature = "k8")]
mod k8;
#[cfg(feature = "k8")]
pub use k8::*;



mod metadata {

    use crate::core::*;
    use super::*;

    impl Spec for TopicSpec {
        const LABEL: &'static str = "Topic";
        type IndexKey = String;
        type Status = TopicStatus;
        type Owner = Self;
    }

    impl Removable for TopicSpec {
        type DeleteKey = String;
    }   

    impl Creatable for TopicSpec {}

    impl Status for TopicStatus {}
}