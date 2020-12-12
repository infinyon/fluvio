use std::fmt;
use std::fmt::Display;

use crate::core::*;
use crate::store::*;
use crate::store::k8::K8MetaItem;

#[derive(PartialEq, Clone)]
pub enum WSAction<S>
where
    S: Spec + PartialEq,
{
    Apply(MetadataStoreObject<S, K8MetaItem>),
    UpdateSpec((S::IndexKey, S)),
    UpdateStatus((S::IndexKey, S::Status)),
    Delete(S::IndexKey),
    DeleteFinal(S::IndexKey)
}

impl<S> fmt::Display for WSAction<S>
where
    S: Spec + PartialEq,
    S::IndexKey: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Apply(obj) => write!(f, "{} Apply: {}", S::LABEL, obj.key),
            Self::UpdateSpec((key, _)) => write!(f, "{} Update Spec: {}", S::LABEL, key),
            Self::UpdateStatus((key, _)) => write!(f, "{} Update Status: {}", S::LABEL, key),
            Self::Delete(key) => write!(f, "{} Delete: {}", S::LABEL, key),
            Self::DeleteFinal(key) => write!(f, "{} Delete Final: {}", S::LABEL, key),
        }
    }
}

impl<S> fmt::Debug for WSAction<S>
where
    S: Spec + PartialEq,
    S::IndexKey: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Apply(obj) => write!(f, "{} Apply {}", S::LABEL, obj.key),
            Self::UpdateSpec((key, _)) => write!(f, "{} Update Spec: {}", S::LABEL, key),
            Self::UpdateStatus((key, _)) => write!(f, "{} Update Status: {}", S::LABEL, key),
            Self::Delete(key) => write!(f, "{} Delete: {}", S::LABEL, key),
            Self::DeleteFinal(key) => write!(f, "{} Delete Final: {}", S::LABEL, key),
        }
    }
}

