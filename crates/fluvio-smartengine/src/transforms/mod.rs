mod filter;
mod map;
mod array_map;
mod filter_map;
mod aggregate;
mod join;
mod join_stream;

mod instance {

    use anyhow::{Result};
    use wasmtime::{ Module};

    use crate::instance::SmartModuleInstance;

    pub(crate) fn create_transform(instance: Module) -> Result<Box<dyn SmartModuleInstance>> {

        let smartmodule_instance: Box<dyn SmartModuleInstance> = match &smart_payload.kind {
            SmartModuleKind::Filter => {
                Box::new(smartmodule.create_filter(smart_payload.params, version)?)
            }
            SmartModuleKind::FilterMap => {
                Box::new(smartmodule.create_filter_map(smart_payload.params, version)?)
            }
            SmartModuleKind::Map => {
                Box::new(smartmodule.create_map(smart_payload.params, version)?)
            }
            SmartModuleKind::ArrayMap => {
                Box::new(smartmodule.create_array_map(smart_payload.params, version)?)
            }
            SmartModuleKind::Join(_) => {
                Box::new(smartmodule.create_join(smart_payload.params, version)?)
            }
            SmartModuleKind::JoinStream {
                topic: _,
                derivedstream: _,
            } => Box::new(smartmodule.create_join_stream(smart_payload.params, version)?),
            SmartModuleKind::Aggregate { accumulator } => Box::new(smartmodule.create_aggregate(
                smart_payload.params,
                accumulator.clone(),
                version,
            )?),
            SmartModuleKind::Generic(context) => {
                smartmodule.create_generic_smartmodule(smart_payload.params, context, version)?
            }
        };

    }

}