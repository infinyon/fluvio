pub use core_model::*;
pub use context::*;

#[cfg(feature = "k8")]
pub use k8::*;

mod context {

    use std::fmt;
    use std::fmt::Display;

    pub type DefaultMetadataContext = MetadataContext<String>;

    pub trait MetadataItem: Clone + Default + fmt::Debug {
        type UId: PartialEq;

        fn uid(&self) -> &Self::UId;

        /// checkif item is newer
        fn is_newer(&self,another: &Self) -> bool;
    }

    impl MetadataItem for String {
        type UId = String;

        fn uid(&self) -> &Self::UId {
            &self
        }


        fn is_newer(&self,_another: &Self) -> bool {
            true
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

    impl<C> Display for MetadataContext<C>
    where
        C: MetadataItem
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{:#?}", self.item)
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

        fn is_newer(&self,another: &Self) -> bool {
            self.resource_version > another.resource_version
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
