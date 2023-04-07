use std::path::Path;
use std::collections::BTreeMap;
use std::io::Read;

use fluvio::{
    SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind, SmartModuleContextData,
};
use fluvio_smartengine::transformation::TransformationConfig;

use flate2::bufread::GzEncoder;
use flate2::Compression;
use anyhow::Result;
use tracing::debug;

/// create smartmodule from predefined name
pub(crate) fn create_smartmodule(
    name: &str,
    ctx: SmartModuleContextData,
    params: BTreeMap<String, String>,
) -> SmartModuleInvocation {
    SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined(name.to_string()),
        kind: SmartModuleKind::Generic(ctx),
        params: params.into(),
    }
}

/// create smartmodule from wasm file
pub(crate) fn create_smartmodule_from_path(
    path: &Path,
    ctx: SmartModuleContextData,
    params: BTreeMap<String, String>,
) -> Result<SmartModuleInvocation> {
    let raw_buffer = std::fs::read(path)?;
    debug!(len = raw_buffer.len(), "read wasm bytes");
    let mut encoder = GzEncoder::new(raw_buffer.as_slice(), Compression::default());
    let mut buffer = Vec::with_capacity(raw_buffer.len());
    encoder.read_to_end(&mut buffer)?;

    Ok(SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::AdHoc(buffer),
        kind: SmartModuleKind::Generic(ctx),
        params: params.into(),
    })
}

/// create list of smartmodules from a list of transformations
pub(crate) fn create_smartmodule_list(
    config: TransformationConfig,
) -> Result<Vec<SmartModuleInvocation>> {
    Ok(config
        .transforms
        .into_iter()
        .map(|t| SmartModuleInvocation {
            wasm: SmartModuleInvocationWasm::Predefined(t.uses),
            kind: SmartModuleKind::Generic(Default::default()),
            params: t
                .with
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<std::collections::BTreeMap<String, String>>()
                .into(),
        })
        .collect())
}
