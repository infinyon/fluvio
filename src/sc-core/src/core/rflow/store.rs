use utils::SimpleConcurrentBTreeMap;

use crate::core::common::KVObject;

use super::FlowSpec;

#[derive(Debug)]
pub struct MemStore<S,P>(SimpleConcurrentBTreeMap<S::Key,KVObject<S,P>>) where S: FlowSpec ;
