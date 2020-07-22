use std::fmt;
use std::fmt::Display;

use flv_metadata::core::*;

use crate::stores::*;

#[derive(Debug, PartialEq, Clone)]
pub enum WSAction<S>
where
    S: Spec + PartialEq,
{
    Apply(MetadataStoreObject<S, K8MetaItem>),
    UpdateSpec((S::IndexKey, S)),
    UpdateStatus((S::IndexKey, S::Status)),

    Delete(S::IndexKey),
}

impl<S> fmt::Display for WSAction<S>
where
    S: Spec + PartialEq,
    S::IndexKey: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Apply(obj) => write!(f, "{} WS Apply Item: {}", S::LABEL, obj.key),
            Self::UpdateSpec((key, _)) => write!(f, "{} WS Update Spec: {}", S::LABEL, key),
            Self::UpdateStatus((key, _)) => write!(f, "{} WS Update Status: {}", S::LABEL, key),
            Self::Delete(key) => write!(f, "{} WS Delete: {}", S::LABEL, key),
        }
    }
}
