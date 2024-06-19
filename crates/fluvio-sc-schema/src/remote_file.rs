#[cfg(feature = "json")]
use std::ops::Deref;

use anyhow::Result;

#[cfg(feature = "use_serde")]
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use fluvio_controlplane_metadata::{mirror::Home, topic::TopicSpec};
use fluvio_stream_model::k8_types::{K8Obj, Spec, ObjectMeta};

#[derive(Debug, Default)]
#[cfg_attr(
    feature = "use_serde",
    derive(Deserialize, Serialize),
    serde(rename_all = "camelCase")
)]
pub struct RemoteMetadata {
    // TODO: remove it, we should get the topics from the upstreams/core
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub topics: Vec<K8Obj<TopicSpec>>,
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub home: Home,
}

/// Configuration used to inihilize a Cluster locally. This data is copied to
/// the K8 cluster metadata
#[derive(Debug, Default)]
#[cfg_attr(
    feature = "use_serde",
    derive(Deserialize, Serialize),
    serde(rename_all = "camelCase")
)]
pub struct RemoteMetadataExport {
    // TODO: remove it, we should get the topics from the upstreams/core
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub topics: Vec<K8ObjExport<TopicSpec>>,
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub home: Home,
}

impl RemoteMetadataExport {
    pub fn new(home: Home) -> Self {
        Self {
            topics: vec![],
            home,
        }
    }
}

impl RemoteMetadata {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}

/// Represents a ClusterConfig that is read from a file. Usually a JSON file.
#[cfg(feature = "json")]
#[derive(Debug, Default)]
pub struct RemoteMetadataFile(RemoteMetadata);

#[cfg(feature = "json")]
impl RemoteMetadataFile {
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)?;

        Self::from_json(&contents)
    }

    fn from_json(json: &str) -> Result<Self> {
        let config: RemoteMetadata = serde_json::from_str(json)?;

        config.validate()?;

        Ok(Self(config))
    }
}

#[cfg(feature = "json")]
impl Deref for RemoteMetadataFile {
    type Target = RemoteMetadata;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "json")]
impl From<RemoteMetadataFile> for RemoteMetadata {
    fn from(file: RemoteMetadataFile) -> Self {
        file.0
    }
}

#[derive(Debug)]
#[cfg_attr(
    feature = "use_serde",
    derive(Deserialize, Serialize),
    serde(rename_all = "camelCase"),
    serde(bound(serialize = "S: Serialize")),
    serde(bound(deserialize = "S: DeserializeOwned"))
)]
pub struct K8ObjExport<S>
where
    S: Spec,
{
    #[cfg_attr(feature = "use_serde", serde(default = "S::api_version"))]
    pub api_version: String,
    #[cfg_attr(feature = "use_serde", serde(default = "S::kind"))]
    pub kind: String,
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub metadata: ObjectMeta,
    #[cfg_attr(feature = "use_serde", serde(default))]
    pub spec: S,
}

impl<S: Spec> From<K8Obj<S>> for K8ObjExport<S> {
    fn from(obj: K8Obj<S>) -> Self {
        Self {
            api_version: obj.api_version,
            kind: obj.kind,
            metadata: obj.metadata,
            spec: obj.spec,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RemoteMetadata;
    #[cfg(feature = "json")]
    use super::RemoteMetadataFile;

    #[cfg(feature = "json")]
    #[test]
    fn validates_json_config() {
        let config = r#"{
            "home": {
                "id": "home",
                "remoteId": "remote1",
                "publicEndpoint": "localhost:30003"
            }
          }
          "#;

        let config = RemoteMetadataFile::from_json(config);

        assert!(config.is_ok());

        let config: RemoteMetadata = config.unwrap().into();

        assert_eq!(config.home.id, "home");
        assert_eq!(config.home.remote_id, "remote1");
        assert_eq!(config.home.public_endpoint, "localhost:30003");
    }
}
