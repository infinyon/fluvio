pub use fluvio_controlplane_metadata::table::*;

mod convert {

    use crate::ObjectDecoder;
    use crate::objects::ListResponse;
    use crate::{
        AdminSpec, NameFilter,
        objects::{
            ListRequest, Metadata, ObjectApiListRequest, ObjectApiListResponse,
            ObjectApiWatchResponse, WatchResponse,
        },
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

    impl From<ListRequest<TableSpec>> for (ObjectApiListRequest, ObjectDecoder) {
        fn from(req: ListRequest<TableSpec>) -> Self {
            (
                ObjectApiListRequest::Table(req),
                TableSpec::object_decoder(),
            )
        }
    }

    impl From<ListResponse<TableSpec>> for (ObjectApiListResponse, ObjectDecoder) {
        fn from(req: ListResponse<TableSpec>) -> Self {
            (
                ObjectApiListResponse::Table(req),
                TableSpec::object_decoder(),
            )
        }
    }
}
