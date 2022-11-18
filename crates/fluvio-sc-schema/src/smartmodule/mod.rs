mod client;
pub use fluvio_controlplane_metadata::smartmodule::*;
pub use client::*;

mod convert {

    use fluvio_controlplane_metadata::smartmodule::{SmartModuleWasmSummary, SmartModuleWasm};

    use crate::{
        AdminSpec, CreatableAdminSpec, DeletableAdminSpec,
        objects::{
            CreateFrom, DeleteRequest, ListRequest, ListResponse, ObjectFrom, ObjectTryFrom,
            WatchRequest, WatchResponse,
        },
    };
    use super::SmartModuleSpec;

    impl AdminSpec for SmartModuleSpec {
        fn summary(self) -> Self {
            Self {
                meta: self.meta,
                summary: Some(SmartModuleWasmSummary {
                    wasm_length: self.wasm.payload.len() as u32,
                }),
                wasm: SmartModuleWasm::default(),
            }
        }
    }

    impl CreatableAdminSpec for SmartModuleSpec {
        const CREATE_TYPE: u8 = 4;
    }

    impl DeletableAdminSpec for SmartModuleSpec {
        type DeleteKey = String;
    }

    CreateFrom!(SmartModuleSpec, SmartModule);
    ObjectFrom!(WatchRequest, SmartModule);
    ObjectFrom!(WatchResponse, SmartModule);
    ObjectFrom!(ListRequest, SmartModule);
    ObjectFrom!(ListResponse, SmartModule);
    ObjectFrom!(DeleteRequest, SmartModule);

    ObjectTryFrom!(WatchResponse, SmartModule);
    ObjectTryFrom!(ListResponse, SmartModule);
}
