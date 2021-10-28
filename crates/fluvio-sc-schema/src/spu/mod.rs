pub use fluvio_controlplane_metadata::spu::*;

mod convert {

    use std::convert::TryFrom;
    use std::convert::TryInto;
    use std::io::Error as IoError;
    use std::io::ErrorKind;

    use fluvio_controlplane_metadata::spu::CustomSpuKey;

    use crate::ObjectDecoder;
    use crate::{
        AdminSpec, NameFilter,
        objects::{
            Metadata, WatchResponse, WatchRequest, ObjectApiWatchRequest, ObjectApiWatchResponse,
        },
    };
    use super::{CustomSpuSpec, SpuSpec};

    impl AdminSpec for CustomSpuSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;

        type DeleteKey = CustomSpuKey;

        type WatchResponseType = Self;

        fn create_decoder() -> crate::CreateDecoder {
            crate::CreateDecoder::CustomSpu
        }
    }

    impl AdminSpec for SpuSpec {
        type ListFilter = NameFilter;
        type ListType = Metadata<Self>;

        type DeleteKey = String;

        type WatchResponseType = Self;

        fn create_decoder() -> crate::CreateDecoder {
            panic!("Spu cannot be created directly")
        }
    }

    impl From<WatchRequest<SpuSpec>> for (ObjectApiWatchRequest, ObjectDecoder) {
        fn from(req: WatchRequest<SpuSpec>) -> Self {
            (ObjectApiWatchRequest::Spu(req), SpuSpec::object_decoder())
        }
    }

    impl From<WatchResponse<SpuSpec>> for ObjectApiWatchResponse {
        fn from(response: WatchResponse<SpuSpec>) -> Self {
            ObjectApiWatchResponse::Spu(response)
        }
    }

    impl TryFrom<ObjectApiWatchResponse> for WatchResponse<SpuSpec> {
        type Error = IoError;

        fn try_from(response: ObjectApiWatchResponse) -> Result<Self, Self::Error> {
            match response {
                ObjectApiWatchResponse::Spu(response) => Ok(response),
                _ => Err(IoError::new(ErrorKind::Other, "not  SPU")),
            }
        }
    }
}
