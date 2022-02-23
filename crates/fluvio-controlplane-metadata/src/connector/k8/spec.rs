//!
//! # Managed Connector Spec
//!
//! Interface to the Managed Connector metadata spec in K8 key value store
//!
use serde::Deserialize;
use serde::Serialize;

use super::super::{ManagedConnectorStatus, SecretString};
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
    pub metadata: K8ManagedConnectorMetadata, // syslog, github star, slack
    pub topic: String,
    pub parameters: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, SecretString>,
}
#[derive(Deserialize, Serialize, Default, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase", default)]
pub struct K8ManagedConnectorMetadata {
    pub image: String,
    pub author: Option<String>,
    pub license: Option<String>,
}
mod convert {

    use crate::connector::*;

    use super::*;

    impl From<K8ManagedConnectorSpec> for ManagedConnectorSpec {
        fn from(spec: K8ManagedConnectorSpec) -> Self {
            Self {
                name: spec.name,
                metadata: spec.metadata.into(),
                topic: spec.topic,
                parameters: spec.parameters,
                secrets: spec.secrets,
                version: spec.version,
            }
        }
    }

    impl From<ManagedConnectorSpec> for K8ManagedConnectorSpec {
        fn from(spec: ManagedConnectorSpec) -> Self {
            Self {
                name: spec.name,
                metadata: spec.metadata.into(),
                topic: spec.topic,
                parameters: spec.parameters,
                secrets: spec.secrets,
                version: spec.version,
            }
        }
    }
    impl From<ManagedConnectorMetadata> for K8ManagedConnectorMetadata {
        fn from(spec: ManagedConnectorMetadata) -> Self {
            Self {
                image: spec.image,
                author: spec.author,
                license: spec.license,
            }
        }
    }
    impl From<K8ManagedConnectorMetadata> for ManagedConnectorMetadata {
        fn from(spec: K8ManagedConnectorMetadata) -> Self {
            Self {
                image: spec.image,
                author: spec.author,
                license: spec.license,
            }
        }
    }

}
