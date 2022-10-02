use serde::Deserialize;
use serde::Serialize;

use crate::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::SmartModuleStatus;
use super::SmartModuleSpec;
use super::SmartModuleWasm;

const SMART_MODULE_API: Crd = Crd {
    group: GROUP,
    version: V1,
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
        &SMART_MODULE_API
    }
}

impl Status for SmartModuleStatus {}



#[derive(Debug, Default, Clone, Eq, PartialEq,Serialize,Deserialize)]
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