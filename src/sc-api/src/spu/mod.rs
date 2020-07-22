pub use flv_metadata::spu::*;

mod convert {

    use std::io::Error;
    use std::io::ErrorKind;
    use std::convert::TryInto;

    use crate::objects::*;
    use super::*;

    impl From<CustomSpuSpec> for AllCreatableSpec {
        fn from(spec: CustomSpuSpec) -> Self {
            Self::CustomSpu(spec)
        }
    }

    impl DeleteSpec for CustomSpuSpec {
        fn into_request<K>(key: K) -> DeleteRequest
        where
            K: Into<Self::DeleteKey>,
        {
            DeleteRequest::CustomSpu(key.into())
        }
    }

    impl ListSpec for SpuSpec {
        type Filter = NameFilter;

        fn into_list_request(filters: Vec<Self::Filter>) -> ListRequest {
            ListRequest::Spu(filters)
        }
    }

    impl TryInto<Vec<Metadata<SpuSpec>>> for ListResponse {
        type Error = Error;

        fn try_into(self) -> Result<Vec<Metadata<SpuSpec>>, Self::Error> {
            match self {
                ListResponse::Spu(s) => Ok(s),
                _ => Err(Error::new(ErrorKind::Other, "not spu")),
            }
        }
    }

    impl ListSpec for CustomSpuSpec {
        type Filter = NameFilter;

        fn into_list_request(filters: Vec<Self::Filter>) -> ListRequest {
            ListRequest::CustomSpu(filters)
        }
    }

    impl TryInto<Vec<Metadata<CustomSpuSpec>>> for ListResponse {
        type Error = Error;

        fn try_into(self) -> Result<Vec<Metadata<CustomSpuSpec>>, Self::Error> {
            match self {
                ListResponse::CustomSpu(s) => Ok(s),
                _ => Err(Error::new(ErrorKind::Other, "not custom spu")),
            }
        }
    }

    impl From<MetadataUpdate<SpuSpec>> for WatchResponse {
        fn from(update: MetadataUpdate<SpuSpec>) -> Self {
            Self::Spu(update)
        }
    }

    impl TryInto<MetadataUpdate<SpuSpec>> for WatchResponse {
        type Error = Error;

        fn try_into(self) -> Result<MetadataUpdate<SpuSpec>, Self::Error> {
            match self {
                WatchResponse::Spu(m) => Ok(m),
                _ => Err(Error::new(ErrorKind::Other, "not  spu")),
            }
        }
    }
}
