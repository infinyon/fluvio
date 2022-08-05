mod spec;
mod status;

pub use self::spec::*;
pub use self::status::*;
use std::fmt;

use fluvio_stream_model::core::MetadataItem;
use fluvio_stream_model::store::MetadataStoreObject;
use fluvio_types::SmartModuleName;
use dataplane::core::{Encoder, Decoder};

#[cfg(feature = "k8")]
mod k8;
#[cfg(feature = "k8")]
pub use k8::*;

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
pub struct SmartModule {
    pub name: SmartModuleName,
    pub input_kind: SmartModuleInputKind,
    pub output_kind: SmartModuleOutputKind,
    pub source_code: Option<SmartModuleSourceCode>,
    pub wasm: SmartModuleWasm,
    pub parameters: Option<Vec<SmartModuleParameter>>,
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
        let SmartModuleSpec {
            input_kind,
            output_kind,
            source_code,
            wasm,
            parameters,
        } = mso.spec;
        Self {
            name,
            input_kind,
            output_kind,
            source_code,
            wasm,
            parameters,
        }
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
