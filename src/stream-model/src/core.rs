pub use core_model::*;
pub use context::*;

#[cfg(feature = "k8")]
pub use k8::*;

mod context {

    use std::fmt::Debug;

    pub type DefaultMetadataContext = MetadataContext<String>;

    pub trait MetadataItem: Clone + Default + Debug {
        type UId: PartialEq;

        fn uid(&self) -> &Self::UId;

        /// update revision if make sense
        fn update_revision(&mut self,revision: String);
    }

    impl MetadataItem for String {
        type UId = String;

        fn uid(&self) -> &Self::UId {
            &self
        }

        fn update_revision(&mut self,_revision: String) {
            // nothing
        }
    }

    #[derive(Default, Debug, Clone, PartialEq)]
    pub struct MetadataContext<C> {
        item: C,
        owner: Option<C>,
    }

    impl<C> From<C> for MetadataContext<C> {
        fn from(item: C) -> Self {
            Self { item, owner: None }
        }
    }

    impl<C> MetadataContext<C> {
        pub fn item(&self) -> &C {
            &self.item
        }

        pub fn item_mut(&mut self) -> &mut C {
            &mut self.item
        }

        pub fn item_owned(self) -> C {
            self.item
        }

        pub fn owner(&self) -> Option<&C> {
            self.owner.as_ref()
        }
        pub fn set_owner(&mut self, ctx: C) {
            self.owner = Some(ctx);
        }
    }

    impl<C> MetadataContext<C>
    where
        C: MetadataItem,
    {
        pub fn create_child(&self) -> Self {
            Self {
                item: C::default(),
                owner: Some(self.item.clone()),
            }
        }
    }
}

#[cfg(feature = "k8")]
pub mod k8 {

    use crate::k8::metadata::ObjectMeta;

    use super::*;

    impl MetadataItem for ObjectMeta {
        type UId = String;
        fn uid(&self) -> &Self::UId {
            &self.uid
        }

        fn update_revision(&mut self,revision: String) {
            self.resource_version = revision;
        }
    }
}

mod core_model {

    use std::fmt::Debug;
    use std::hash::Hash;

    /// metadata driver
    pub trait MetadataStoreDriver {
        type Metadata;
    }

    pub trait Spec: Default + Debug + Clone + PartialEq {
        const LABEL: &'static str;
        type Status: Status;
        type Owner: Spec;
        type IndexKey: Debug + Eq + Hash + Clone + ToString;
    }

    pub trait Status: Default + Debug + Clone + PartialEq {}

    /// for deleting objects
    pub trait Removable {
        type DeleteKey;
    }

    /// marker trait for creating
    pub trait Creatable {}

    /// Represents some metadata object
    pub struct MetadataObj<S, P>
    where
        P: MetadataStoreDriver,
        S: Spec,
    {
        pub name: String,
        pub metadata: P::Metadata,
        pub spec: S,
        pub status: S::Status,
    }
}
