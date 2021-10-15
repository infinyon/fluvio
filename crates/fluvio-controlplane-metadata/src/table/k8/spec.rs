//!
//! # Table display spec
//!
//! Interface to Table metadata spec in K8 key value store
//!
use serde::Deserialize;
use serde::Serialize;

use super::super::TableStatus;
use super::super::InputFormat;
use crate::k8_types::{Spec, Crd, DefaultHeader};
//use std::collections::BTreeMap;

use crd::TABLE_SPEC_API;
mod crd {

    use crate::k8_types::{Crd, CrdNames, GROUP, V1};

    pub const TABLE_SPEC_API: Crd = Crd {
        group: GROUP,
        version: V1,
        names: CrdNames {
            kind: "Table",
            plural: "tables",
            singular: "table",
        },
    };
}

impl Spec for K8TableSpec {
    type Status = TableStatus;
    type Header = DefaultHeader;
    fn metadata() -> &'static Crd {
        &TABLE_SPEC_API
    }
}

#[derive(Deserialize, Serialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase", default)]
pub struct K8TableSpec {
    pub name: String,
    pub input_format: Option<InputFormat>,
    //pub column: TableColumnConfig,
    pub smartmodule: Option<String>,
}
mod convert {

    use crate::table::*;

    use super::*;

    impl From<K8TableSpec> for TableSpec {
        fn from(spec: K8TableSpec) -> Self {
            Self {
                name: spec.name,
                input_format: None,
                smartmodule: None,
            }
        }
    }

    impl From<TableSpec> for K8TableSpec {
        fn from(spec: TableSpec) -> Self {
            Self {
                name: spec.name,
                input_format: None,
                smartmodule: None,
            }
        }
    }
}
