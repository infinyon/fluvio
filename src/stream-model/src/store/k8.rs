use std::io::Error as IoError;
use std::io::ErrorKind;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::num::ParseIntError;
use std::fmt::Display;
use std::fmt::Debug;
use std::ops::Deref;

use crate::k8::metadata::Spec as K8Spec;
use crate::k8::metadata::Status as K8Status;
use crate::k8::metadata::ObjectMeta;
use crate::k8::metadata::K8Obj;
use crate::store::{MetadataStoreObject};
use crate::core::{Spec, MetadataItem, MetadataContext};

pub type K8MetadataContext = MetadataContext<K8MetaItem>;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct K8MetaItem {
    revision: u64,
    inner: ObjectMeta,
}

impl K8MetaItem {
    pub fn new<S>(name: S, name_space: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            revision: 0,
            inner: ObjectMeta::new(name, name_space),
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

impl MetadataItem for K8MetaItem {
    type UId = String;
    fn uid(&self) -> &Self::UId {
        &self.inner.uid
    }

    #[inline]
    fn is_newer(&self, another: &Self) -> bool {
        self.revision > another.revision
    }
}

impl TryFrom<ObjectMeta> for K8MetaItem {
    type Error = ParseIntError;

    fn try_from(value: ObjectMeta) -> Result<Self, Self::Error> {
        if value.resource_version.is_empty() {
            return Ok(Self {
                revision: 0,
                inner: value,
            });
        }
        let revision: u64 = value.resource_version.parse()?;

        Ok(Self {
            revision,
            inner: value,
        })
    }
}

#[derive(Debug)]
pub enum K8ConvertError<S>
where
    S: K8Spec,
{
    /// skip, this object, it is not considered valid object  
    Skip(K8Obj<S>),
    /// Converting error
    KeyConvertionError(IoError),
    Other(IoError),
}

/// trait to convert type object to our spec
pub trait K8ExtendedSpec: Spec {
    type K8Spec: K8Spec;
    type K8Status: K8Status;

    fn convert_from_k8(
        k8_obj: K8Obj<Self::K8Spec>,
    ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>>;
}

pub fn default_convert_from_k8<S>(
    k8_obj: K8Obj<S::K8Spec>,
) -> Result<MetadataStoreObject<S, K8MetaItem>, K8ConvertError<S::K8Spec>>
where
    S: K8ExtendedSpec,
    S::IndexKey: TryFrom<String> + Display,
    <S::IndexKey as TryFrom<String>>::Error: Debug,
    <<S as K8ExtendedSpec>::K8Spec as K8Spec>::Status: Into<S::Status>,
    S::K8Spec: Into<S>,
{
    let k8_name = k8_obj.metadata.name.clone();
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
                    let ctx: MetadataContext<K8MetaItem> = ctx_item.into();
                    let local_kv =
                        MetadataStoreObject::new(key, local_spec, local_status).with_context(ctx);

                    Ok(local_kv)
                }
                Err(err) => Err(K8ConvertError::KeyConvertionError(IoError::new(
                    ErrorKind::InvalidData,
                    format!("error converting metadata: {:#?}", err),
                ))),
            }
        }
        Err(err) => Err(K8ConvertError::KeyConvertionError(IoError::new(
            ErrorKind::InvalidData,
            format!("error converting key: {:#?}", err),
        ))),
    }
}
