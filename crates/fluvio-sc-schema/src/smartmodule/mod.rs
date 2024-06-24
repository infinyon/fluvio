mod client;
pub use fluvio_controlplane_metadata::smartmodule::*;
pub use client::*;

mod convert {

    use fluvio_controlplane_metadata::smartmodule::{SmartModuleWasmSummary, SmartModuleWasm};

    use crate::{AdminSpec, CreatableAdminSpec, DeletableAdminSpec, UpdatableAdminSpec};
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

    impl CreatableAdminSpec for SmartModuleSpec {}

    impl DeletableAdminSpec for SmartModuleSpec {
        type DeleteKey = String;
    }

    impl UpdatableAdminSpec for SmartModuleSpec {
        type UpdateKey = String;
        type UpdateAction = String;
    }
}
