pub use fluvio_controlplane_metadata::table::*;

mod convert {

    use crate::objects::CreateType;
    use crate::objects::Metadata;
    use crate::objects::MetadataUpdate;
    use crate::{AdminSpec, NameFilter};
    use super::TableSpec;

    impl AdminSpec for TableSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = MetadataUpdate<Self>;

        type DeleteKey = String;
    }
}
