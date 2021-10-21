pub use fluvio_controlplane_metadata::smartstream::*;

mod convert {

    use std::io::Error;
    use std::io::ErrorKind;
    use std::convert::TryInto;

    use crate::objects::*;
    use super::*;

    impl From<SmartStreamSpec> for AllCreatableSpec {
        fn from(spec: SmartStreamSpec) -> Self {
            Self::SmartStream(spec)
        }
    }

    impl DeleteSpec for SmartStreamSpec {
        fn into_request<K>(key: K) -> DeleteRequest
        where
            K: Into<Self::DeleteKey>,
        {
            DeleteRequest::SmartStream(key.into())
        }
    }

    impl ListSpec for SmartStreamSpec {
        type Filter = NameFilter;

        fn into_list_request(filters: Vec<Self::Filter>) -> ListRequest {
            ListRequest::SmartStream(filters)
        }
    }

    impl TryInto<Vec<Metadata<SmartStreamSpec>>> for ListResponse {
        type Error = Error;

        fn try_into(self) -> Result<Vec<Metadata<SmartStreamSpec>>, Self::Error> {
            match self {
                ListResponse::SmartStream(s) => Ok(s),
                _ => Err(Error::new(ErrorKind::Other, "not smartmodule")),
            }
        }
    }

    impl From<MetadataUpdate<SmartStreamSpec>> for WatchResponse {
        fn from(update: MetadataUpdate<SmartStreamSpec>) -> Self {
            Self::SmartStream(update)
        }
    }

    impl TryInto<MetadataUpdate<SmartStreamSpec>> for WatchResponse {
        type Error = Error;

        fn try_into(self) -> Result<MetadataUpdate<SmartStreamSpec>, Self::Error> {
            match self {
                WatchResponse::SmartStream(m) => Ok(m),
                _ => Err(Error::new(ErrorKind::Other, "not smartmodule")),
            }
        }
    }

}
