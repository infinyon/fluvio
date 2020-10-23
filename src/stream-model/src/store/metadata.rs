use std::fmt::Debug;

use crate::core::{Spec, MetadataContext, MetadataItem};

pub type DefaultMetadataObject<S> = MetadataStoreObject<S, u32>;

use super::DualDiff;
use super::MetadataChange;

#[derive(Debug, Clone, PartialEq)]
pub struct MetadataStoreObject<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    pub spec: S,
    pub status: S::Status,
    pub key: S::IndexKey,
    pub ctx: MetadataContext<C>,
}

impl<S, C> MetadataStoreObject<S, C>
where
    S: Spec,
    C: MetadataItem,
    S::Status: Default,
{
    pub fn new<J>(key: J, spec: S, status: S::Status) -> Self
    where
        J: Into<S::IndexKey>,
    {
        Self {
            key: key.into(),
            spec,
            status,
            ctx: MetadataContext::default(),
        }
    }

    pub fn new_with_context<J>(key: J, spec: S, ctx: MetadataContext<C>) -> Self
    where
        J: Into<S::IndexKey>,
    {
        Self {
            key: key.into(),
            spec,
            status: S::Status::default(),
            ctx,
        }
    }

    pub fn with_spec<J>(key: J, spec: S) -> Self
    where
        J: Into<S::IndexKey>,
        C: Default,
    {
        Self::new(key.into(), spec, S::Status::default())
    }

    pub fn with_key<J>(key: J) -> Self
    where
        J: Into<S::IndexKey>,
    {
        Self::with_spec(key.into(), S::default())
    }

    pub fn with_context(mut self, ctx: impl Into<MetadataContext<C>>) -> Self {
        self.ctx = ctx.into();
        self
    }

    pub fn key(&self) -> &S::IndexKey {
        &self.key
    }

    pub fn key_owned(&self) -> S::IndexKey {
        self.key.clone()
    }

    pub fn my_key(self) -> S::IndexKey {
        self.key
    }

    pub fn spec(&self) -> &S {
        &self.spec
    }

    // set spec
    pub fn set_spec(&mut self, spec: S) {
        self.spec = spec;
    }

    pub fn status(&self) -> &S::Status {
        &self.status
    }

    pub fn set_status(&mut self, status: S::Status) {
        self.status = status;
    }

    pub fn ctx(&self) -> &MetadataContext<C> {
        &self.ctx
    }

    pub fn ctx_mut(&mut self) -> &mut MetadataContext<C> {
        &mut self.ctx
    }

    pub fn ctx_owned(&self) -> MetadataContext<C> {
        self.ctx.clone()
    }

    pub fn set_ctx(&mut self, ctx: MetadataContext<C>) {
        self.ctx = ctx;
    }

    pub fn parts(self) -> (S::IndexKey, S, S::Status, MetadataContext<C>) {
        (self.key, self.spec, self.status, self.ctx)
    }

    /// check if metadata is owned by other
    pub fn is_owned(&self, uid: &C::UId) -> bool {
        match self.ctx().owner() {
            Some(parent) => parent.uid() == uid,
            None => false,
        }
    }

    pub fn is_newer(&self, another: &Self) -> bool {
        self.ctx.item().is_newer(another.ctx().item())
    }
}

impl<S, C> DualDiff for MetadataStoreObject<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    /// compute difference, in our case we take account of version as well
    fn diff(&self, another: &Self) -> MetadataChange {
        if self.is_newer(another) {
            MetadataChange::no_change()
        } else {
            MetadataChange {
                spec: self.spec != another.spec,
                status: self.status != another.status,
            }
        }
    }
}

impl<S, C> Into<(S::IndexKey, S, S::Status)> for MetadataStoreObject<S, C>
where
    S: Spec,
    C: MetadataItem,
{
    fn into(self) -> (S::IndexKey, S, S::Status) {
        (self.key, self.spec, self.status)
    }
}
