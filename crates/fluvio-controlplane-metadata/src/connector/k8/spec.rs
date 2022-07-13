//!
//! # Managed Connector Spec
//!
//! Interface to the Managed Connector metadata spec in K8 key value store
//!
use serde::Deserialize;
use serde::Serialize;

use super::super::{ManagedConnectorStatus, SecretString, ManagedConnectorParameterValue};
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
    pub version: String,
    #[cfg_attr(feature = "use_serde", serde(rename = "type"))]
    pub type_: String, // syslog, github star, slack
    pub topic: String,
    pub parameters: BTreeMap<String, ManagedConnectorParameterValue>,
    pub secrets: BTreeMap<String, SecretString>,
}

mod convert {

    use crate::connector::*;

    use super::*;

    impl From<K8ManagedConnectorSpec> for ManagedConnectorSpec {
        fn from(spec: K8ManagedConnectorSpec) -> Self {
            Self {
                name: spec.name,
                type_: spec.type_,
                topic: spec.topic,
                parameters: spec.parameters,
                secrets: spec.secrets,
                version: spec.version.into(),
            }
        }
    }

    impl From<ManagedConnectorSpec> for K8ManagedConnectorSpec {
        fn from(spec: ManagedConnectorSpec) -> Self {
            Self {
                name: spec.name,
                type_: spec.type_,
                topic: spec.topic,
                parameters: spec.parameters,
                secrets: spec.secrets,
                version: spec.version.to_string(),
            }
        }
    }
}
