//!
//! # SPU Spec
//!
//! Interface to the SPU metadata spec in K8 key value store
//!
use serde::Deserialize;
use serde::Serialize;

use fluvio_types::defaults::{SPU_PUBLIC_PORT, SPU_LOG_BASE_DIR, SPU_LOG_SIZE};
use fluvio_types::defaults::SPU_PRIVATE_PORT;

use crate::spu::EncryptionEnum;
use crate::spg::SpuGroupStatus;
use crate::k8_types::{Spec, Crd, DefaultHeader, TemplateSpec, Env};

use crd::SPG_API;
mod crd {

    use crate::k8_types::{Crd, CrdNames, GROUP, V1};

    pub const SPG_API: Crd = Crd {
        group: GROUP,
        version: V1,
        names: CrdNames {
            kind: "SpuGroup",
            plural: "spugroups",
            singular: "spugroup",
        },
    };
}

impl Spec for K8SpuGroupSpec {
    type Status = SpuGroupStatus;
    type Header = DefaultHeader;
    fn metadata() -> &'static Crd {
        &SPG_API
    }
}

#[derive(Deserialize, Serialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase", default)]
pub struct K8SpuGroupSpec {
    pub template: TemplateSpec<SpuTemplate>,
    pub replicas: u16,
    #[serde(default)]
    pub min_id: i32,
}

#[derive(Deserialize, Serialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase", default)]
pub struct SpuTemplate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rack: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_endpoint: Option<SpuEndpointTemplate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_endpoint: Option<SpuEndpointTemplate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication: Option<K8ReplicationConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<K8StorageConfig>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<Env>,
}

#[derive(Deserialize, Serialize, Default, Debug, Eq, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpuEndpointTemplate {
    pub port: u16,
    pub encryption: EncryptionEnum,
}

impl SpuEndpointTemplate {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            encryption: EncryptionEnum::PLAINTEXT,
        }
    }

    pub fn default_public() -> Self {
        Self {
            port: SPU_PUBLIC_PORT,
            encryption: EncryptionEnum::PLAINTEXT,
        }
    }

    pub fn default_private() -> Self {
        Self {
            port: SPU_PRIVATE_PORT,
            encryption: EncryptionEnum::PLAINTEXT,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Eq, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ControllerEndPoint {
    pub port: u16,
    pub hoste: String,
    pub encryption: EncryptionEnum,
}

#[derive(Deserialize, Default, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct K8ReplicationConfig {
    pub in_sync_replica_min: Option<u16>,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct K8StorageConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
}

impl K8StorageConfig {
    pub fn log_dir(&self) -> String {
        self.log_dir
            .clone()
            .unwrap_or_else(|| SPU_LOG_BASE_DIR.to_owned())
    }

    pub fn size(&self) -> String {
        self.size.clone().unwrap_or_else(|| SPU_LOG_SIZE.to_owned())
    }
}

mod convert {

    use crate::spg::*;

    use super::*;

    impl From<K8SpuGroupSpec> for SpuGroupSpec {
        fn from(spec: K8SpuGroupSpec) -> Self {
            Self {
                replicas: spec.replicas,
                min_id: spec.min_id,
                spu_config: spec.template.spec.into(),
            }
        }
    }

    impl From<SpuTemplate> for SpuConfig {
        fn from(template: SpuTemplate) -> Self {
            Self {
                rack: template.rack,
                replication: template.replication.map(|r| r.into()),
                storage: template.storage.map(|s| s.into()),
                env: vec![], // doesn't really matter
            }
        }
    }

    impl From<SpuGroupSpec> for K8SpuGroupSpec {
        fn from(spec: SpuGroupSpec) -> Self {
            Self {
                replicas: spec.replicas,
                min_id: spec.min_id,
                template: TemplateSpec {
                    spec: spec.spu_config.into(),
                    ..Default::default()
                },
            }
        }
    }

    impl From<SpuConfig> for SpuTemplate {
        fn from(config: SpuConfig) -> Self {
            Self {
                rack: config.rack,
                storage: config.storage.map(|s| s.into()),
                replication: config.replication.map(|s| s.into()),
                ..Default::default()
            }
        }
    }

    impl From<StorageConfig> for K8StorageConfig {
        fn from(storage: StorageConfig) -> Self {
            Self {
                log_dir: storage.log_dir,
                size: storage.size,
            }
        }
    }

    impl From<K8StorageConfig> for StorageConfig {
        fn from(config: K8StorageConfig) -> Self {
            Self {
                log_dir: config.log_dir,
                size: config.size,
            }
        }
    }

    impl From<ReplicationConfig> for K8ReplicationConfig {
        fn from(config: ReplicationConfig) -> Self {
            Self {
                in_sync_replica_min: config.in_sync_replica_min,
            }
        }
    }

    impl From<K8ReplicationConfig> for ReplicationConfig {
        fn from(config: K8ReplicationConfig) -> Self {
            Self {
                in_sync_replica_min: config.in_sync_replica_min,
            }
        }
    }
}
