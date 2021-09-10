pub use fluvio_controlplane_metadata::managed_connector::*;

mod convert {

    use std::io::Error;
    use std::io::ErrorKind;
    use std::convert::TryInto;

    use crate::objects::*;
    use super::*;

    impl From<ManagedConnectorSpec> for AllCreatableSpec {
        fn from(spec: ManagedConnectorSpec) -> Self {
            Self::ManagedConnector(spec)
        }
    }

    impl DeleteSpec for ManagedConnectorSpec {
        fn into_request<K>(key: K) -> DeleteRequest
        where
            K: Into<Self::DeleteKey>,
        {
            DeleteRequest::ManagedConnector(key.into())
        }
    }

    impl ListSpec for ManagedConnectorSpec {
        type Filter = NameFilter;

        fn into_list_request(filters: Vec<Self::Filter>) -> ListRequest {
            ListRequest::ManagedConnector(filters)
        }
    }

    impl TryInto<Vec<Metadata<ManagedConnectorSpec>>> for ListResponse {
        type Error = Error;

        fn try_into(self) -> Result<Vec<Metadata<ManagedConnectorSpec>>, Self::Error> {
            match self {
                ListResponse::ManagedConnector(s) => Ok(s),
                _ => Err(Error::new(ErrorKind::Other, "not managed connector")),
            }
        }
    }

    impl From<MetadataUpdate<ManagedConnectorSpec>> for WatchResponse {
        fn from(update: MetadataUpdate<ManagedConnectorSpec>) -> Self {
            Self::ManagedConnector(update)
        }
    }

    impl TryInto<MetadataUpdate<ManagedConnectorSpec>> for WatchResponse {
        type Error = Error;

        fn try_into(self) -> Result<MetadataUpdate<ManagedConnectorSpec>, Self::Error> {
            match self {
                WatchResponse::ManagedConnector(m) => Ok(m),
                _ => Err(Error::new(ErrorKind::Other, "not managed connector")),
            }
        }
    }
    /*
     */
}
