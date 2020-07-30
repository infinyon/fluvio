use std::io::Error as IoError;
use std::io::ErrorKind;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt::Display;
use std::fmt::Debug;

use crate::k8::metadata::Spec as K8Spec;
use crate::k8::metadata::Status as K8Status;
use crate::k8::metadata::ObjectMeta;
use crate::k8::metadata::K8Obj;
use crate::store::*;
use crate::core::*;

pub type K8MetaItem = ObjectMeta;
pub type K8MetadataContext = MetadataContext<K8MetaItem>;

/// trait to convert type object to our spec
pub trait K8ExtendedSpec: Spec {
    type K8Spec: K8Spec;
    type K8Status: K8Status;

    fn convert_from_k8(
        k8_obj: K8Obj<Self::K8Spec>,
    ) -> Result<MetadataStoreObject<Self, K8MetaItem>, IoError>
    where
        Self::IndexKey: TryFrom<String> + Display,
        <Self::IndexKey as TryFrom<String>>::Error: Debug,
        <<Self as K8ExtendedSpec>::K8Spec as K8Spec>::Status: Into<Self::Status>,
        Self::K8Spec: Into<Self>,
    {
        let k8_name = k8_obj.metadata.name.clone();
        let result: Result<Self::IndexKey, _> = k8_name.try_into();
        match result {
            Ok(key) => {
                // convert K8 Spec/Status into Metadata Spec/Status
                let local_spec = k8_obj.spec.into();
                let local_status = k8_obj.status.into();

                let ctx: MetadataContext<ObjectMeta> = k8_obj.metadata.into();
                let local_kv =
                    MetadataStoreObject::new(key, local_spec, local_status).with_context(ctx);

                Ok(local_kv)
            }
            Err(err) => Err(IoError::new(
                ErrorKind::InvalidData,
                format!("error converting key: {:#?}", err),
            )),
        }
    }
}