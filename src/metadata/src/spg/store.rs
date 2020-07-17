//!
//! Spu Group
//!

use crate::store::*;


use super::*;

pub type SpuGroupMetadata<C> = MetadataStoreObject<SpuGroupSpec,C>;

pub type SpuGroupLocalStore<C> = LocalStore<SpuGroupSpec,C>;
