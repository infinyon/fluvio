pub use fluvio_controlplane_metadata::smartmodule::*;

mod convert {

    use std::io::Error;
    use std::io::ErrorKind;
    use std::convert::TryInto;

    use crate::objects::*;
    use super::*;

    impl From<SmartModuleSpec> for AllCreatableSpec {
        fn from(spec: SmartModuleSpec) -> Self {
            Self::SmartModule(spec)
        }
    }

    impl DeleteSpec for SmartModuleSpec {
        fn into_request<K>(key: K) -> DeleteRequest
        where
            K: Into<Self::DeleteKey>,
        {
            DeleteRequest::SmartModule(key.into())
        }
    }

    impl ListSpec for SmartModuleSpec {
        type Filter = NameFilter;

        fn into_list_request(filters: Vec<Self::Filter>) -> ListRequest {
            ListRequest::SmartModule(filters)
        }
    }

    impl TryInto<Vec<Metadata<SmartModuleSpec>>> for ListResponse {
        type Error = Error;

        fn try_into(self) -> Result<Vec<Metadata<SmartModuleSpec>>, Self::Error> {
            match self {
                ListResponse::SmartModule(s) => Ok(s),
                _ => Err(Error::new(ErrorKind::Other, "not smartmodule")),
            }
        }
    }

    impl From<MetadataUpdate<SmartModuleSpec>> for WatchResponse {
        fn from(update: MetadataUpdate<SmartModuleSpec>) -> Self {
            Self::SmartModule(update)
        }
    }

    impl TryInto<MetadataUpdate<SmartModuleSpec>> for WatchResponse {
        type Error = Error;

        fn try_into(self) -> Result<MetadataUpdate<SmartModuleSpec>, Self::Error> {
            match self {
                WatchResponse::SmartModule(m) => Ok(m),
                _ => Err(Error::new(ErrorKind::Other, "not smartmodule")),
            }
        }
    }
    /*
     */
}
