use std::fmt;
use std::fmt::Display;

use crate::core::*;
use crate::store::*;
use crate::store::k8::K8MetaItem;

#[derive(PartialEq, Clone)]
pub enum WSAction<S, MetaContext = K8MetaItem>
where
    S: Spec + PartialEq,
    MetaContext: MetadataItem,
{
    Apply(MetadataStoreObject<S, MetaContext>),
    UpdateSpec((S::IndexKey, S)),
    UpdateStatus((S::IndexKey, S::Status)),
    Delete(S::IndexKey),
    DeleteFinal(S::IndexKey),
}

impl<S, MetaContext> fmt::Display for WSAction<S, MetaContext>
where
    S: Spec + PartialEq,
    S::IndexKey: Display,
    MetaContext: MetadataItem,
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

impl<S, MetaContext> fmt::Debug for WSAction<S, MetaContext>
where
    S: Spec + PartialEq,
    S::IndexKey: Display,
    MetaContext: MetadataItem,
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
