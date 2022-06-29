//!
//! # Managed Connector Spec
//!
//! Interface to the Managed Connector metadata spec in K8 key value store
//!
use serde::Deserialize;
use serde::Serialize;

use super::super::{ManagedConnectorStatus, SecretString, VecOrString};
use crate::k8_types::{Spec, Crd, DefaultHeader};
use std::collections::BTreeMap;

use crd::MANAGED_CONNECTOR_API;
mod crd {

    use crate::k8_types::{Crd, CrdNames, GROUP, V1};

    pub const MANAGED_CONNECTOR_API: Crd = Crd {
        group: GROUP,
        version: V1,
        names: CrdNames {
            kind: "ManagedConnector",
            plural: "managedconnectors",
            singular: "managedconnector",
        },
    };
}

impl Spec for K8ManagedConnectorSpec {
    type Status = ManagedConnectorStatus;
    type Header = DefaultHeader;
    fn metadata() -> &'static Crd {
        &MANAGED_CONNECTOR_API
    }
}

#[derive(Deserialize, Serialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase", default)]
pub struct K8ManagedConnectorSpec {
    pub name: String,
    pub version: Option<String>,
    #[cfg_attr(feature = "use_serde", serde(rename = "type"))]
    pub type_: String, // syslog, github star, slack
    pub topic: String,
    pub parameters: BTreeMap<String, VecOrString>,
    pub secrets: BTreeMap<String, SecretString>,
}

mod convert {

    use crate::connector::*;

    use super::*;

    impl From<K8ManagedConnectorSpec> for ManagedConnectorSpec {
        fn from(spec: K8ManagedConnectorSpec) -> Self {
            let mut parameters_old = BTreeMap::new();
            let parameters = spec.parameters;
            for (key, value) in &parameters {
                if let VecOrString::String(ref value) = value {
                    parameters_old.insert(key.clone(), value.clone());
                }
            }
            Self {
                name: spec.name,
                type_: spec.type_,
                topic: spec.topic,
                parameters,
                secrets: spec.secrets,
                version: spec.version,
                parameters_old,
            }
        }
    }

    impl From<ManagedConnectorSpec> for K8ManagedConnectorSpec {
        fn from(spec: ManagedConnectorSpec) -> Self {
            let mut parameters = spec.parameters;
            for (key, value) in spec.parameters_old {
                parameters.insert(key, VecOrString::String(value));
            }
            Self {
                name: spec.name,
                type_: spec.type_,
                topic: spec.topic,
                parameters,
                secrets: spec.secrets,
                version: spec.version,
            }
        }
    }
}
