// implementation of metadata client do nothing
// it is used for testing where to satisfy metadata contract
use std::io::Error as IoError;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;

use futures::stream::StreamExt;
use futures::stream::BoxStream;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use k8_diff::DiffError;
use metadata_core::Spec;
use metadata_core::metadata::K8Meta;
use metadata_core::metadata::InputK8Obj;
use metadata_core::metadata::UpdateK8ObjStatus;
use metadata_core::metadata::K8List;
use metadata_core::metadata::K8Obj;
use metadata_core::metadata::K8Status;
use metadata_core::metadata::K8Watch;

use crate::MetadataClient;
use crate::MetadataClientError;
use crate::TokenStreamResult;

#[derive(Debug)]
pub enum DoNothingError {
    IoError(IoError),
    DiffError(DiffError),
    JsonError(serde_json::Error),
    PatchError,
    NotFound,
}

impl From<IoError> for DoNothingError {
    fn from(error: IoError) -> Self {
        Self::IoError(error)
    }
}

impl From<serde_json::Error> for DoNothingError {
    fn from(error: serde_json::Error) -> Self {
        Self::JsonError(error)
    }
}

impl From<DiffError> for DoNothingError {
    fn from(error: DiffError) -> Self {
        Self::DiffError(error)
    }
}

impl fmt::Display for DoNothingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "io: {}", err),
            Self::JsonError(err) => write!(f, "{}", err),
            Self::NotFound => write!(f, "not found"),
            Self::DiffError(err) => write!(f, "{:#?}", err),
            Self::PatchError => write!(f, "patch error"),
        }
    }
}

impl MetadataClientError for DoNothingError {
    fn patch_error() -> Self {
        Self::PatchError
    }

    fn not_founded(&self) -> bool {
        match self {
            Self::NotFound => true,
            _ => false,
        }
    }
}

pub struct DoNothingClient();

#[async_trait]
impl MetadataClient for DoNothingClient {
    type MetadataClientError = DoNothingError;

    async fn retrieve_item<S, M>(
        &self,
        _metadata: &M,
    ) -> Result<K8Obj<S, S::Status>, Self::MetadataClientError>
    where
        K8Obj<S, S::Status>: DeserializeOwned,
        S: Spec,
        M: K8Meta<S> + Send + Sync,
    {
        Err(DoNothingError::NotFound) as Result<K8Obj<S, S::Status>, Self::MetadataClientError>
    }

    async fn retrieve_items<S>(
        &self,
        _namespace: &str,
    ) -> Result<K8List<S, S::Status>, Self::MetadataClientError>
    where
        K8List<S, S::Status>: DeserializeOwned,
        S: Spec,
    {
        Err(DoNothingError::NotFound) as Result<K8List<S, S::Status>, Self::MetadataClientError>
    }

    async fn delete_item<S, M>(&self, _metadata: &M) -> Result<K8Status, Self::MetadataClientError>
    where
        S: Spec,
        M: K8Meta<S> + Send + Sync,
    {
        Err(DoNothingError::NotFound) as Result<K8Status, Self::MetadataClientError>
    }

    async fn create_item<S>(
        &self,
        _value: InputK8Obj<S>,
    ) -> Result<K8Obj<S, S::Status>, Self::MetadataClientError>
    where
        InputK8Obj<S>: Serialize + Debug,
        K8Obj<S, S::Status>: DeserializeOwned,
        S: Spec + Send,
    {
        Err(DoNothingError::NotFound) as Result<K8Obj<S, S::Status>, Self::MetadataClientError>
    }

    async fn update_status<S>(
        &self,
        _value: &UpdateK8ObjStatus<S, S::Status>,
    ) -> Result<K8Obj<S, S::Status>, Self::MetadataClientError>
    where
        UpdateK8ObjStatus<S, S::Status>: Serialize + Debug,
        K8Obj<S, S::Status>: DeserializeOwned,
        S: Spec + Send + Sync,
        S::Status: Send + Sync,
    {
        Err(DoNothingError::NotFound) as Result<K8Obj<S, S::Status>, Self::MetadataClientError>
    }

    async fn patch_spec<S, M>(
        &self,
        _metadata: &M,
        _patch: &Value,
    ) -> Result<K8Obj<S, S::Status>, Self::MetadataClientError>
    where
        K8Obj<S, S::Status>: DeserializeOwned,
        S: Spec + Debug,
        M: K8Meta<S> + Display + Send + Sync,
    {
        Err(DoNothingError::NotFound) as Result<K8Obj<S, S::Status>, Self::MetadataClientError>
    }

    fn watch_stream_since<S>(
        &self,
        _namespace: &str,
        _resource_version: Option<String>,
    ) -> BoxStream<'_, TokenStreamResult<S, S::Status, Self::MetadataClientError>>
    where
        K8Watch<S, S::Status>: DeserializeOwned,
        S: Spec + Debug + Send + 'static,
        S::Status: Debug + Send,
    {
        futures::stream::empty().boxed()
    }
}
