//!
//! Spu Group
//!

use crate::store::*;

use super::*;

pub type SpuGroupMetadata<C> = MetadataStoreObject<ManagedConnectorSpec, C>;

pub type SpuGroupLocalStore<C> = LocalStore<ManagedConnectorSpec, C>;
