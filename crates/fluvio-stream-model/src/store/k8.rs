use std::io::Error as IoError;
use std::io::ErrorKind;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::num::ParseIntError;
use std::fmt::Display;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::cmp::PartialEq;
use std::collections::HashMap;

use tracing::error;

use crate::k8_types::{Spec as K8Spec, ObjectMeta, K8Obj};
use crate::store::MetadataStoreObject;
use crate::core::{Spec, MetadataItem, MetadataContext};

pub type K8MetadataContext = MetadataContext<K8MetaItem>;

#[derive(Debug, Default, Clone)]
pub struct K8MetaItem {
    revision: u64,
    inner: ObjectMeta,
    owner: Option<Box<K8MetaItem>>,
}

/// for sake of comparison, we only care about couple of fields in the metadata
impl PartialEq for K8MetaItem {
    fn eq(&self, other: &Self) -> bool {
        self.inner.uid == other.inner.uid
            && self.inner.labels == other.inner.labels
            && self.inner.owner_references == other.inner.owner_references
            && self.inner.annotations == other.inner.annotations
            && self.inner.finalizers == other.inner.finalizers
            && self.deletion_grace_period_seconds == other.inner.deletion_grace_period_seconds
            && self.deletion_timestamp == other.deletion_timestamp
    }
}

impl K8MetaItem {
    pub fn new<S>(name: S, name_space: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            revision: 0,
            inner: ObjectMeta::new(name, name_space),
            owner: Default::default(),
        }
    }

    pub fn inner(&self) -> &ObjectMeta {
        &self.inner
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }
}

impl Deref for K8MetaItem {
    type Target = ObjectMeta;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for K8MetaItem {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl MetadataItem for K8MetaItem {
    type UId = String;
    fn uid(&self) -> &Self::UId {
        &self.inner.uid
    }

    #[inline]
    fn is_newer(&self, another: &Self) -> bool {
        self.revision >= another.revision
    }

    fn is_being_deleted(&self) -> bool {
        self.inner.deletion_grace_period_seconds.is_some()
    }

    fn set_labels<T: Into<String>>(mut self, labels: Vec<(T, T)>) -> Self {
        let inner = self.inner.set_labels(labels);
        self.inner = inner;
        self
    }

    /// get string labels
    fn get_labels(&self) -> HashMap<String, String> {
        self.inner.labels.clone()
    }

    fn owner(&self) -> Option<&Self> {
        self.owner.as_ref().map(|w| w.as_ref())
    }

    fn set_owner(&mut self, owner: Self) {
        self.owner = Some(Box::new(owner))
    }
}

impl TryFrom<ObjectMeta> for K8MetaItem {
    type Error = ParseIntError;

    fn try_from(value: ObjectMeta) -> Result<Self, Self::Error> {
        let revision: u64 = if value.resource_version.is_empty() {
            0
        } else {
            value.resource_version.parse()?
        };
        if value.owner_references.len() > 1 {
            error!("too many owners: {value:#?}");
        }
        let owner = if let Some(owner_ref) = value.owner_references.first() {
            let inner = ObjectMeta {
                name: owner_ref.name.to_owned(),
                namespace: value.namespace.to_owned(),
                uid: owner_ref.uid.to_owned(),
                ..Default::default()
            };
            Some(Box::new(Self {
                inner,
                ..Default::default()
            }))
        } else {
            None
        };

        Ok(Self {
            revision,
            inner: value,
            owner,
        })
    }
}

#[derive(Debug)]
pub enum K8ConvertError<S>
where
    S: K8Spec,
{
    /// skip, this object, it is not considered valid object  
    Skip(Box<K8Obj<S>>),
    /// Converting error
    KeyConvertionError(IoError),
    Other(IoError),
}

/// trait to convert type object to our spec
pub trait K8ExtendedSpec: Spec {
    type K8Spec: K8Spec + Send + Sync;

    // if true, use foreground delete
    const DELETE_WAIT_DEPENDENTS: bool = false;
    const FINALIZER: Option<&'static str> = None;

    fn convert_from_k8(
        k8_obj: K8Obj<Self::K8Spec>,
        multi_namespace_context: bool,
    ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>>;

    fn convert_status_from_k8(status: Self::Status) -> <Self::K8Spec as K8Spec>::Status;

    fn into_k8(self) -> Self::K8Spec;
}

/// converts typical K8 objects into metadata store objects
/// when in multi namespace context, keys are prefixed with namespace
pub fn default_convert_from_k8<S>(
    k8_obj: K8Obj<S::K8Spec>,
    multi_namespace_context: bool,
) -> Result<MetadataStoreObject<S, K8MetaItem>, K8ConvertError<S::K8Spec>>
where
    S: K8ExtendedSpec,
    S::IndexKey: TryFrom<String> + Display,
    <S::IndexKey as TryFrom<String>>::Error: Debug,
    <<S as K8ExtendedSpec>::K8Spec as K8Spec>::Status: Into<S::Status>,
    S::K8Spec: Into<S>,
{
    let k8_name = if multi_namespace_context {
        format!("{}.{}", k8_obj.metadata.namespace, k8_obj.metadata.name)
    } else {
        k8_obj.metadata.name.clone()
    };

    let result: Result<S::IndexKey, _> = k8_name.try_into();
    match result {
        Ok(key) => {
            // convert K8 Spec/Status into Metadata Spec/Status
            let local_spec = k8_obj.spec.into();
            let local_status = k8_obj.status.into();

            let ctx_item_result: Result<K8MetaItem, _> = k8_obj.metadata.try_into();
            match ctx_item_result {
                Ok(ctx_item) => {
                    //   trace!("k8 revision: {}, meta revision: {}",ctx_item.revision(),ctx_item.inner().resource_version);
                    Ok(MetadataStoreObject::new(key, local_spec, local_status)
                        .with_context(MetadataContext::new(ctx_item)))
                }
                Err(err) => Err(K8ConvertError::KeyConvertionError(IoError::new(
                    ErrorKind::InvalidData,
                    format!("error converting metadata: {err:#?}"),
                ))),
            }
        }
        Err(err) => Err(K8ConvertError::KeyConvertionError(IoError::new(
            ErrorKind::InvalidData,
            format!("error converting key: {err:#?}"),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use k8_types::OwnerReferences;

    use super::*;

    #[test]
    fn test_k8_meta_from_object_meta() {
        //given
        let object_meta = ObjectMeta {
            name: "w2".to_owned(),
            namespace: "default".to_owned(),
            resource_version: "123".to_string(),
            owner_references: vec![OwnerReferences {
                kind: "Widget".to_owned(),
                name: "w1".to_owned(),
                uid: "13fb7d10-6f1e-4749-8e9d-7f6c4013b8a3".to_owned(),
                ..Default::default()
            }],
            ..Default::default()
        };

        //when
        let k8_meta: K8MetaItem = object_meta.try_into().expect("converted");

        //then
        assert_eq!(k8_meta.revision, 123);
        assert_eq!(k8_meta.inner.name, "w2");
        assert_eq!(k8_meta.inner.namespace, "default");
        assert_eq!(k8_meta.inner.owner_references.len(), 1);

        let owner = k8_meta.owner.expect("owner");
        assert_eq!(owner.inner.uid, "13fb7d10-6f1e-4749-8e9d-7f6c4013b8a3");
        assert_eq!(owner.inner.name, "w1");
    }

    #[test]
    fn test_k8_meta_from_object_meta_empty_resource_version() {
        //given
        let object_meta = ObjectMeta {
            name: "w2".to_owned(),
            owner_references: vec![OwnerReferences {
                name: "w1".to_owned(),
                ..Default::default()
            }],
            ..Default::default()
        };

        //when
        let k8_meta: K8MetaItem = object_meta.try_into().expect("converted");

        //then
        assert_eq!(k8_meta.revision, 0);
        assert_eq!(k8_meta.inner.name, "w2");
        assert_eq!(k8_meta.inner.owner_references.len(), 1);

        let owner = k8_meta.owner.expect("owner");
        assert_eq!(owner.inner.name, "w1");
    }
}
