pub use fluvio_controlplane_metadata::table::*;

mod convert {

    use std::io::Error;
    use std::io::ErrorKind;
    use std::convert::TryInto;

    use crate::objects::*;
    use super::*;

    impl From<TableSpec> for AllCreatableSpec {
        fn from(spec: TableSpec) -> Self {
            Self::Table(spec)
        }
    }

    impl DeleteSpec for TableSpec {
        fn into_request<K>(key: K) -> DeleteRequest
        where
            K: Into<Self::DeleteKey>,
        {
            DeleteRequest::Table(key.into())
        }
    }

    impl ListSpec for TableSpec {
        type Filter = NameFilter;

        fn into_list_request(filters: Vec<Self::Filter>) -> ListRequest {
            ListRequest::Table(filters)
        }
    }

    impl TryInto<Vec<Metadata<TableSpec>>> for ListResponse {
        type Error = Error;

        fn try_into(self) -> Result<Vec<Metadata<TableSpec>>, Self::Error> {
            match self {
                ListResponse::Table(s) => Ok(s),
                _ => Err(Error::new(ErrorKind::Other, "not table")),
            }
        }
    }

    impl From<MetadataUpdate<TableSpec>> for WatchResponse {
        fn from(update: MetadataUpdate<TableSpec>) -> Self {
            Self::Table(update)
        }
    }

    impl TryInto<MetadataUpdate<TableSpec>> for WatchResponse {
        type Error = Error;

        fn try_into(self) -> Result<MetadataUpdate<TableSpec>, Self::Error> {
            match self {
                WatchResponse::Table(m) => Ok(m),
                _ => Err(Error::new(ErrorKind::Other, "not table")),
            }
        }
    }
    /*
     */
}
