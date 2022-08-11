use std::fmt::Debug;
use std::fmt::Display;
use std::fmt;

use crate::core::*;
use crate::store::*;

/// changes that will be applied to store
/// add/update has same effect
pub enum LSUpdate<S, C>
where
    S: Spec,
    S::Status: PartialEq,
    C: MetadataItem,
{
    Mod(MetadataStoreObject<S, C>),
    Delete(S::IndexKey),
}

/// changes that happened in the store
#[derive(Debug, PartialEq, Clone)]
pub enum LSChange<S, C>
where
    S: Spec,
    S::Status: PartialEq,
    C: MetadataItem,
{
    Add(MetadataStoreObject<S, C>),
    Mod(MetadataStoreObject<S, C>, MetadataStoreObject<S, C>), // new, old
    Delete(MetadataStoreObject<S, C>),
}

impl<S, C> fmt::Display for LSChange<S, C>
where
    S: Spec,
    S::IndexKey: Display,
    S::Status: PartialEq,
    C: MetadataItem,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Add(add) => write!(f, "{} Add: {}", S::LABEL, add.key()),
            Self::Mod(update, _) => write!(f, "{} Mod: {}", S::LABEL, update.key()),
            Self::Delete(del) => write!(f, "{} Delete: {}", S::LABEL, del.key()),
        }
    }
}

impl<S, C> LSChange<S, C>
where
    S: Spec,
    S::Status: PartialEq,
    C: MetadataItem,
{
    pub fn add<K>(value: K) -> Self
    where
        K: Into<MetadataStoreObject<S, C>>,
    {
        LSChange::Add(value.into())
    }

    pub fn update(new: MetadataStoreObject<S, C>, old: MetadataStoreObject<S, C>) -> Self {
        LSChange::Mod(new, old)
    }

    pub fn delete(value: MetadataStoreObject<S, C>) -> Self {
        LSChange::Delete(value)
    }
}

/// change in spec
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum SpecChange<S>
where
    S: Spec,
{
    Add(S),
    Mod(S, S),
    Delete(S),
}

impl<S, C> From<LSChange<S, C>> for SpecChange<S>
where
    S: Spec,
    S::Status: PartialEq,
    C: MetadataItem,
{
    fn from(meta: LSChange<S, C>) -> Self {
        match meta {
            LSChange::Add(val) => Self::Add(val.spec),
            LSChange::Mod(new, old) => Self::Mod(new.spec, old.spec),
            LSChange::Delete(old) => Self::Delete(old.spec),
        }
    }
}
