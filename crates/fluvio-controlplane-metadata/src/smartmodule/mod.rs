mod spec;
mod status;
mod package;
mod params;
mod spec_v1;

pub use self::spec::*;
pub use self::status::*;
pub use self::package::*;

use std::fmt;

use fluvio_stream_model::core::MetadataItem;
use fluvio_stream_model::store::MetadataStoreObject;
use fluvio_types::SmartModuleName;
use fluvio_protocol::{Encoder, Decoder};

#[cfg(feature = "k8")]
mod k8;
#[cfg(feature = "k8")]
pub use k8::*;

/// SmartModule object that can be used to transport from SC to SPU
#[derive(Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
pub struct SmartModule {
    pub name: SmartModuleName,
    pub spec: SmartModuleSpec,
}

impl fmt::Display for SmartModule {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModule({})", self.name)
    }
}

impl<C> From<MetadataStoreObject<SmartModuleSpec, C>> for SmartModule
where
    C: MetadataItem,
{
    fn from(mso: MetadataStoreObject<SmartModuleSpec, C>) -> Self {
        let name = mso.key_owned();
        let spec = mso.spec;
        Self { name, spec }
    }
}

mod metadata {

    use crate::core::{Spec, Status, Removable, Creatable};
    use crate::extended::{SpecExt, ObjectType};

    use super::*;

    impl Spec for SmartModuleSpec {
        const LABEL: &'static str = "SmartModule";
        type IndexKey = String;
        type Status = SmartModuleStatus;
        type Owner = Self;
    }

    impl SpecExt for SmartModuleSpec {
        const OBJECT_TYPE: ObjectType = ObjectType::SmartModule;
    }

    impl Removable for SmartModuleSpec {
        type DeleteKey = String;
    }

    impl Creatable for SmartModuleSpec {}

    impl Status for SmartModuleStatus {}

    #[cfg(feature = "k8")]
    mod extended {

        use crate::store::k8::K8ExtendedSpec;
        use crate::store::k8::K8ConvertError;
        use crate::store::k8::K8MetaItem;
        use crate::store::MetadataStoreObject;
        use crate::k8_types::K8Obj;
        use crate::store::k8::default_convert_from_k8;

        use super::SmartModuleSpec;

        impl K8ExtendedSpec for SmartModuleSpec {
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
