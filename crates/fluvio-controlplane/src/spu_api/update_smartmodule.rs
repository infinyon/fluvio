use std::fmt;

use fluvio_controlplane_metadata::core::MetadataItem;
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::store::MetadataStoreObject;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;
use fluvio_protocol::api::Request;
use fluvio_types::SmartModuleName;

use crate::requests::ControlPlaneRequest;

use super::api::InternalSpuApi;

pub type UpdateSmartModuleRequest = ControlPlaneRequest<SmartModule>;

impl Request for UpdateSmartModuleRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateSmartModule as u16;
    type Response = UpdateSmartModuleResponse;
    const DEFAULT_API_VERSION: i16 = 10; // align with pubic api to get version encoding
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateSmartModuleResponse {}

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
