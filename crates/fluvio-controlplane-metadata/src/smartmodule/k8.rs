use serde::Deserialize;
use serde::Serialize;

use crate::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::SmartModuleStatus;
use super::SmartModuleSpec;
use super::SmartModuleWasm;

const V2: &str = "v2";

const SMART_MODULE_V2_API: Crd = Crd {
    group: GROUP,
    version: V2,
    names: CrdNames {
        kind: "SmartModule",
        plural: "smartmodules",
        singular: "smartmodule",
    },
};

impl Spec for SmartModuleSpec {
    type Status = SmartModuleStatus;
    type Header = DefaultHeader;

    fn metadata() -> &'static Crd {
        &SMART_MODULE_V2_API
    }
}

impl Status for SmartModuleStatus {}

#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SmartModuleSpecV1 {
    pub input_kind: SmartModuleInputKind,
    pub output_kind: SmartModuleOutputKind,
    pub wasm: SmartModuleWasm,
    pub parameters: Option<Vec<SmartModuleParameter>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum SmartModuleInputKind {
    Stream,
    External,
}

impl Default for SmartModuleInputKind {
    fn default() -> SmartModuleInputKind {
        SmartModuleInputKind::Stream
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum SmartModuleOutputKind {
    Stream,
    External,
    Table,
}

impl Default for SmartModuleOutputKind {
    fn default() -> SmartModuleOutputKind {
        SmartModuleOutputKind::Stream
    }
}

impl std::fmt::Display for SmartModuleSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SmartModuleSpec")
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]

pub struct SmartModuleParameter {
    name: String,
}

const SMART_MODULE_V1_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "SmartModule",
        plural: "smartmodules",
        singular: "smartmodule",
    },
};

/// SmartModuleV1 could be empty
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SmartModuleV1Wrapper {
    #[serde(flatten)]
    pub inner: Option<SmartModuleSpecV1>,
}

impl Spec for SmartModuleV1Wrapper {
    type Status = SmartModuleStatus;
    type Header = DefaultHeader;

    fn metadata() -> &'static Crd {
        &SMART_MODULE_V1_API
    }
}

impl From<SmartModuleSpecV1> for SmartModuleSpec {
    fn from(v1: SmartModuleSpecV1) -> Self {
        Self {
            wasm: v1.wasm,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::smartmodule::SmartModuleInputKind;

    #[test]
    fn test_sm_spec_v1_simple() {
        use super::SmartModuleSpecV1;

        let yaml_spec: &str = r#"
input_kind: Stream
output_kind: Stream
wasm:
    format: BINARY
    payload: H4sIAAAAAAA
"#;
        let sm_spec: SmartModuleSpecV1 =
            serde_yaml::from_str(yaml_spec).expect("Failed to deserialize");

        assert_eq!(sm_spec.input_kind, SmartModuleInputKind::Stream);
    }
}
