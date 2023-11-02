pub use core_model::*;
pub use context::*;

mod context {

    use std::fmt;
    use std::fmt::Display;
    use std::collections::HashMap;

    pub type DefaultMetadataContext = MetadataContext<String>;

    pub trait MetadataItem:
        Clone + Default + fmt::Debug + PartialEq + Send + Sync + 'static
    {
        type UId: PartialEq;

        fn uid(&self) -> &Self::UId;

        /// checkif item is newer
        fn is_newer(&self, another: &Self) -> bool;

        /// if object is process of being deleted
        fn is_being_deleted(&self) -> bool {
            false
        }

        /// set string labels
        fn set_labels<T: Into<String>>(self, _labels: Vec<(T, T)>) -> Self {
            self
        }

        /// get string labels
        fn get_labels(&self) -> HashMap<String, String> {
            HashMap::new()
        }

        fn owner(&self) -> Option<&Self> {
            Default::default()
        }

        fn set_owner(&mut self, _owner: Self) {}

        fn children(&self) -> Option<&HashMap<String, Vec<Self>>> {
            Default::default()
        }

        fn set_children(&mut self, _children: HashMap<String, Vec<Self>>) {}
    }

    pub trait MetadataRevExtension: MetadataItem {
        // return next revision
        fn next_rev(&self) -> Self;
    }

    impl MetadataItem for u32 {
        type UId = u32;

        fn uid(&self) -> &Self::UId {
            self
        }

        fn is_newer(&self, another: &Self) -> bool {
            self > another
        }
    }

    impl MetadataItem for u64 {
        type UId = u64;

        fn uid(&self) -> &Self::UId {
            self
        }

        fn is_newer(&self, another: &Self) -> bool {
            self > another
        }
    }

    #[derive(Default, Debug, Clone, Eq, PartialEq)]
    pub struct MetadataContext<C> {
        item: C,
    }

    impl<C> From<C> for MetadataContext<C> {
        fn from(item: C) -> Self {
            Self { item }
        }
    }

    impl<C> MetadataContext<C> {
        pub fn new(item: C) -> Self {
            Self::from(item)
        }

        pub fn item(&self) -> &C {
            &self.item
        }

        pub fn item_mut(&mut self) -> &mut C {
            &mut self.item
        }

        pub fn set_item(&mut self, item: C) {
            self.item = item;
        }

        pub fn item_owned(self) -> C {
            self.item
        }

        pub fn into_inner(self) -> C {
            self.item
        }
    }

    impl<C> MetadataContext<C>
    where
        C: MetadataItem,
    {
        pub fn create_child(&self) -> Self {
            let mut item = C::default();
            item.set_owner(self.item().clone());
            Self { item }
        }

        pub fn set_labels<T: Into<String>>(mut self, labels: Vec<(T, T)>) -> Self {
            let item = self.item.set_labels(labels);
            self.item = item;
            self
        }
    }

    impl<C> MetadataContext<C>
    where
        C: MetadataRevExtension,
    {
        pub fn next_rev(&self) -> Self {
            Self::new(self.item.next_rev())
        }
    }

    impl<C> Display for MetadataContext<C>
    where
        C: MetadataItem,
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{:#?}", self.item)
        }
    }
}

mod core_model {

    use std::fmt::{Debug, Display};
    use std::hash::Hash;
    use std::str::FromStr;

    /// metadata driver
    pub trait MetadataStoreDriver {
        type Metadata;
    }

    #[cfg(not(feature = "use_serde"))]
    pub trait Spec: Default + Debug + Clone + PartialEq + Send + Sync + 'static {
        const LABEL: &'static str;
        type Status: Status;
        type Owner: Spec;
        type IndexKey: Debug + Eq + Hash + Clone + ToString + FromStr + Display + Send + Sync;
    }

    #[cfg(feature = "use_serde")]
    pub trait Spec:
        Default
        + Debug
        + Clone
        + PartialEq
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static
    {
        const LABEL: &'static str;
        type Status: Status + serde::Serialize + serde::de::DeserializeOwned;
        type Owner: Spec;
        type IndexKey: Debug + Eq + Hash + Clone + ToString + FromStr + Display + Send + Sync;
    }

    pub trait Status:
        Default + Debug + Clone + ToString + Display + PartialEq + Send + Sync
    {
    }

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
