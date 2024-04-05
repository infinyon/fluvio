use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

#[derive(Debug, Clone, PartialEq, Eq, Default, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct RemoteSpec {
    pub remote_type: RemoteType,
    // TODO: we should add auth
}

impl RemoteSpec {
    pub fn type_name(&self) -> &str {
        match &self.remote_type {
            RemoteType::Edge(_) => "edge",
            RemoteType::Core(_) => "core",
        }
    }
}

impl fmt::Display for RemoteSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Remote: {:?}", self)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RemoteType {
    #[cfg_attr(feature = "use_serde", serde(rename = "edge"))]
    #[fluvio(tag = 0)]
    Edge(Edge),
    #[cfg_attr(feature = "use_serde", serde(rename = "core"))]
    #[fluvio(tag = 1)]
    Core(Core),
}

impl fmt::Display for RemoteType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ts = match self {
            RemoteType::Edge(edge) => format!("edge: {}", edge.id),
            RemoteType::Core(core) => format!("core: {}", core.id),
        };
        write!(f, "{}", ts)
    }
}

impl Default for RemoteType {
    fn default() -> Self {
        Self::Edge(Edge::default())
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct Edge {
    pub id: String,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct Core {
    pub id: String,
    pub public_endpoint: String,
}
