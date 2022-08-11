mod concurrent_hashmap;
pub mod actions;
mod metadata;
mod filter;
mod dual_store;
pub mod event;

#[cfg(feature = "k8")]
pub mod k8;

pub use filter::*;
pub use concurrent_hashmap::*;
pub use metadata::*;
pub use dual_store::*;

// re-export epoch
pub use crate::epoch::*;

/// simple memory based metadata
pub mod memory {

    use crate::core::{MetadataItem, MetadataRevExtension};

    /// simple memory representation of meta
    #[derive(Debug, Default, Eq, PartialEq, Clone)]
    pub struct MemoryMeta {
        pub rev: u32,
    }

    impl MetadataItem for MemoryMeta {
        type UId = u32;

        fn uid(&self) -> &Self::UId {
            &self.rev
        }

        fn is_newer(&self, another: &Self) -> bool {
            self.rev >= another.rev
        }
    }

    impl MetadataRevExtension for MemoryMeta {
        fn next_rev(&self) -> Self {
            Self { rev: self.rev + 1 }
        }
    }

    impl MemoryMeta {
        #[allow(unused)]
        pub fn new(rev: u32) -> Self {
            Self { rev }
        }
    }
}
