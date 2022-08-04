mod spec;
mod status;

pub use self::spec::*;
pub use self::status::*;

#[cfg(feature = "k8")]
mod k8;
#[cfg(feature = "k8")]
pub use k8::*;

mod metadata {

    use crate::core::{Spec, Status, Removable, Creatable};
    use crate::extended::{SpecExt, ObjectType};

    use super::*;

    impl Spec for DerivedStreamSpec {
        const LABEL: &'static str = "DerivedStream";
        type IndexKey = String;
        type Status = DerivedStreamStatus;
        type Owner = Self;
    }

    impl SpecExt for DerivedStreamSpec {
        const OBJECT_TYPE: ObjectType = ObjectType::Topic;
    }

    impl Removable for DerivedStreamSpec {
        type DeleteKey = String;
    }

    impl Creatable for DerivedStreamSpec {}

    impl Status for DerivedStreamStatus {}

    #[allow(clippy::enum_variant_names)]
    #[derive(thiserror::Error, Debug)]
    pub enum DerivedStreamValidationError {
        #[error("Topic not found: {0}")]
        TopicNotFound(String),
        #[error("DerivedStream not found: {0}")]
        DerivedStreamNotFound(String),
        #[error("SmartModule not found: {0}")]
        SmartModuleNotFound(String),
    }

    #[cfg(feature = "k8")]
    mod extended {

        use crate::store::k8::K8ExtendedSpec;
        use crate::store::k8::K8ConvertError;
        use crate::store::k8::K8MetaItem;
        use crate::store::MetadataStoreObject;
        use crate::k8_types::K8Obj;
        use crate::store::k8::default_convert_from_k8;

        use super::DerivedStreamSpec;

        impl K8ExtendedSpec for DerivedStreamSpec {
            type K8Spec = Self;
            type K8Status = Self::Status;

            fn convert_from_k8(
                k8_obj: K8Obj<Self::K8Spec>,
                multi_namespace_context: bool,
            ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>>
            {
                default_convert_from_k8(k8_obj, multi_namespace_context)
            }
        }
    }
}
