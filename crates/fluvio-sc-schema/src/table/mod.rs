pub use fluvio_controlplane_metadata::table::*;

mod convert {

    use crate::objects::{ListResponse, ObjectFrom, ObjectTryFrom, WatchRequest};
    use crate::{
        AdminSpec, NameFilter,
        objects::{ListRequest, Metadata, WatchResponse},
    };
    use super::TableSpec;

    impl AdminSpec for TableSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;
        type WatchResponseType = Self;

        type DeleteKey = String;

        fn create_decoder() -> crate::CreateDecoder {
            crate::CreateDecoder::TABLE
        }
    }

    ObjectFrom!(WatchRequest, Table);
    ObjectFrom!(WatchResponse, Table);

    ObjectFrom!(ListRequest, Table);
    ObjectFrom!(ListResponse, Table);

    ObjectTryFrom!(WatchResponse, Table);
    ObjectTryFrom!(ListResponse, Table);
}
