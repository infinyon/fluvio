
use serde::Serialize;

use metadata_core::Crd;
use metadata_core::metadata::K8Obj;




#[derive(Debug)]
pub enum ApplyResult<S,P>
{
    None,
    Created(K8Obj<S,P>),
    Patched(K8Obj<S,P>)
}

#[allow(dead_code)]
pub enum PatchMergeType {
    Json,
    JsonMerge,
    StrategicMerge, // for aggegration API
}

impl PatchMergeType {
    pub fn for_spec(crd: &Crd) -> Self {
        match crd.group {
            "core" => PatchMergeType::StrategicMerge,
            "apps" => PatchMergeType::StrategicMerge,
            _ => PatchMergeType::JsonMerge,
        }
    }

    pub fn content_type(&self) -> &'static str {
        match self {
            PatchMergeType::Json => "application/json-patch+json",
            PatchMergeType::JsonMerge => "application/merge-patch+json",
            PatchMergeType::StrategicMerge => "application/strategic-merge-patch+json",
        }
    }
}

/// used for comparing spec,
#[derive(Serialize, Debug, Clone)]
pub struct DiffSpec<S> {
    spec: S,
}

impl<S> DiffSpec<S>
where
    S: Serialize,
{
    pub fn from(spec: S) -> Self {
        DiffSpec { spec }
    }
}