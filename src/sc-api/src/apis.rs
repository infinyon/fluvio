//!
//! # SC Api Keys
//!
//! Stores Api Keys supported by the SC.
//!

use kf_protocol::derive::Encode;
use kf_protocol::derive::Decode;

/// API call from client to SPU
#[derive(Encode, Decode, PartialEq, Debug, Clone, Copy)]
#[repr(u16)]
pub enum ScPublicApiKey {
    // Mixed
    ApiVersion = 18,

    // Kafka
    KfMetadata = 3,

    // update metadata async
    //  FlvUpdateAllMetadata = 1000,
    //  FlvUpdateSpuMetadata = 1001,
    //  FlvUpdateReplicaMetadata = 1002,

    // Topics
    FlvCreateTopics = 2001,
    FlvDeleteTopics = 2002,
    FlvFetchTopics = 2003,
    FlvTopicComposition = 2004,

    // Custom SPUs
    FlvRegisterCustomSpus = 2005,
    FlvUnregisterCustomSpus = 2006,
    FlvFetchSpus = 2007,

    // SPU Groups
    FlvCreateSpuGroups = 2008,
    FlvDeleteSpuGroups = 2009,
    FlvFetchSpuGroups = 2010,
}

impl Default for ScPublicApiKey {
    fn default() -> Self {
        Self::ApiVersion
    }
}
