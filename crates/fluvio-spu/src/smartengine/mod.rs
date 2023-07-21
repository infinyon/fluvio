use std::collections::BTreeMap;

use fluvio::{
    SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind, SmartModuleExtraParams,
};
use fluvio_controlplane_metadata::topic::Deduplication;
use fluvio_protocol::link::ErrorCode;
use fluvio_smartengine::EngineError;
use fluvio_smartmodule::dataplane::smartmodule::Lookback;

pub(crate) mod batch;
pub(crate) mod file_batch;
pub(crate) mod produce_batch;
pub(crate) mod context;
mod chain;

pub(crate) fn dedup_to_invocation(dedup: &Deduplication) -> SmartModuleInvocation {
    let lookback = Lookback {
        last: dedup.bounds.count,
        age: dedup.bounds.age,
    };
    let mut params = BTreeMap::new();
    params.insert("count".to_owned(), dedup.bounds.count.to_string());
    if let Some(age) = dedup.bounds.age {
        params.insert("age".to_string(), age.as_millis().to_string());
    };
    for (k, v) in dedup.filter.transform.with.iter() {
        params.insert(k.clone(), v.clone());
    }
    SmartModuleInvocation {
        wasm: SmartModuleInvocationWasm::Predefined(dedup.filter.transform.uses.clone()),
        kind: SmartModuleKind::Filter,
        params: SmartModuleExtraParams::new(params, Some(lookback)),
    }
}

pub(crate) fn map_engine_error(err: &EngineError) -> ErrorCode {
    match err {
        EngineError::UnknownSmartModule => ErrorCode::Other("Unknown SmartModule type".to_string()),
        EngineError::Instantiate(err) => ErrorCode::Other(err.to_string()),
        EngineError::StoreMemoryExceeded {
            current: _,
            requested,
            max,
        } => ErrorCode::SmartModuleMemoryLimitExceeded {
            requested: *requested as u64,
            max: *max as u64,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fluvio_controlplane_metadata::topic::{Bounds, Filter, Transform};

    use super::*;

    #[test]
    fn test_dedup_to_inv() {
        //when
        let dedup = Deduplication {
            bounds: Bounds {
                count: 1,
                age: Some(Duration::from_secs(1)),
            },
            filter: Filter {
                transform: Transform {
                    uses: "filter@0.1.0".to_string(),
                    with: BTreeMap::from([("param_name".to_string(), "param_value".to_string())]),
                },
            },
        };
        //when
        let inv = dedup_to_invocation(&dedup);

        //then
        assert!(matches!(
            inv.wasm,
            SmartModuleInvocationWasm::Predefined(str) if str.eq("filter@0.1.0")
        ));
        assert!(matches!(inv.kind, SmartModuleKind::Filter));
        assert_eq!(inv.params.get("count"), Some(&"1".to_string()));
        assert_eq!(inv.params.get("age"), Some(&"1000".to_string()));
        assert_eq!(
            inv.params.get("param_name"),
            Some(&"param_value".to_string())
        );
    }
}
