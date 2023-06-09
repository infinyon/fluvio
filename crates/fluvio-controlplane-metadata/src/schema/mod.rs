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

    impl Spec for SchemaSpec {
        const LABEL: &'static str = "Schema";
        type IndexKey = String;
        type Status = SchemaStatus;
        type Owner = Self;
    }

    impl SpecExt for SchemaSpec {
        const OBJECT_TYPE: ObjectType = ObjectType::Topic;
    }

    impl Removable for SchemaSpec {
        type DeleteKey = String;
    }

    impl Creatable for SchemaSpec {}

    impl Status for SchemaStatus {}

    #[cfg(feature = "k8")]
    mod extended {

        use crate::store::k8::K8ExtendedSpec;
        use crate::store::k8::K8ConvertError;
        use crate::store::k8::K8MetaItem;
        use crate::store::MetadataStoreObject;
        use crate::k8_types::K8Obj;
        use crate::store::k8::default_convert_from_k8;

        use super::SchemaSpec;

        impl K8ExtendedSpec for SchemaSpec {
            type K8Spec = Self;
            type K8Status = Self::Status;

            const DELETE_WAIT_DEPENDENTS: bool = true;

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
