//!
//! # Fetch SPU Groups
//!
//! Public API to fetch SPU Group metadata from the SC
//!
use kf_protocol::api::Request;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use k8_metadata::spg::SpuGroupSpec;
use k8_metadata::spg::SpuGroupStatus;
use k8_metadata::spg::SpuGroupStatusResolution;
use k8_metadata::spg::SpuTemplate;
use k8_metadata::metadata::TemplateSpec;
use k8_metadata::metadata::K8Obj;
use k8_metadata::spg::StorageConfig;

use super::ScServerApiKey;
use super::FlvResponseMessage;

use super::spu::FlvSpuGroupResolution;

/// Fetch SPU Groups by type
#[derive(Decode, Encode, Default, Debug)]
pub struct FlvFetchSpuGroupsRequest {}

impl Request for FlvFetchSpuGroupsRequest {
    const API_KEY: u16 = ScServerApiKey::FlvFetchSpuGroups as u16;
    const DEFAULT_API_VERSION: i16 = 1;
    type Response = FlvFetchSpuGroupsResponse;
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvFetchSpuGroupsResponse {
    pub error: FlvResponseMessage,
    /// Each spu in the response.
    pub spu_groups: Vec<FlvFetchSpuGroup>,
}

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvFetchSpuGroup {
    pub name: String,

    /// The number of replicas for the spu group
    pub replicas: u16,

    // The base spu id for the spu group
    pub min_id: i32,

    /// Rack label, optional parameter used by replica assignment algorithm.
    pub rack: Option<String>,

    /// storage size
    pub size: String,

    /// Status resolution
    pub resolution: FlvSpuGroupResolution,

    /// Reason for Status resolution (if applies)
    pub reason: Option<String>,
}

impl Into<(String, SpuGroupSpec, SpuGroupStatus)> for FlvFetchSpuGroup {
    fn into(self) -> (String, SpuGroupSpec, SpuGroupStatus) {
        (
            self.name,
            SpuGroupSpec {
                replicas: self.replicas,
                min_id: Some(self.min_id),
                template: TemplateSpec {
                    spec: SpuTemplate {
                        rack: self.rack,
                        storage: Some(StorageConfig {
                            size: Some(self.size),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            SpuGroupStatus {
                resolution: self.resolution.into(),
                ..Default::default()
            },
        )
    }
}

impl From<K8Obj<SpuGroupSpec>> for FlvFetchSpuGroup {
    fn from(item: K8Obj<SpuGroupSpec>) -> Self {
        let (name, spec, status) = (item.metadata.name, item.spec, item.status);
        let min_id = spec.min_id();
        let (replicas, template) = (spec.replicas, spec.template.spec);
        let (rack, storage) = (template.rack, template.storage.unwrap_or_default());
        Self {
            name,
            replicas,
            min_id,
            rack,
            size: storage.size(),
            resolution: status.resolution.into(),
            reason: None,
        }
    }
}

impl From<SpuGroupStatusResolution> for FlvSpuGroupResolution {
    fn from(res: SpuGroupStatusResolution) -> Self {
        match res {
            SpuGroupStatusResolution::Init => FlvSpuGroupResolution::Init,
            SpuGroupStatusResolution::Invalid => FlvSpuGroupResolution::Invalid,
            SpuGroupStatusResolution::Reserved => FlvSpuGroupResolution::Reserved,
        }
    }
}

impl Into<SpuGroupStatusResolution> for FlvSpuGroupResolution {
    fn into(self) -> SpuGroupStatusResolution {
        match self {
            Self::Init => SpuGroupStatusResolution::Init,
            Self::Invalid => SpuGroupStatusResolution::Invalid,
            Self::Reserved => SpuGroupStatusResolution::Reserved,
        }
    }
}
