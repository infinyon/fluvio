pub use fluvio_controlplane_metadata::spu::{CustomSpuSpec, CustomSpuKey};

use crate::objects::CreateRequest;
use crate::objects::DeleteRequest;
use crate::objects::ListRequest;
use crate::objects::ListResponse;
use crate::{
    AdminSpec, NameFilter,
    objects::{ObjectFrom, ObjectTryFrom, Metadata, WatchResponse, WatchRequest},
};

impl AdminSpec for CustomSpuSpec {
    type ListFilter = NameFilter;
    type ListType = Metadata<Self>;

    type DeleteKey = CustomSpuKey;

    type WatchResponseType = Self;

    const CREATE_TYPE: u8 =1;

    fn create_decoder() -> crate::CreateDecoder {
        crate::CreateDecoder::CustomSpu
    }
}

ObjectFrom!(CreateRequest, CustomSpu, Create);
ObjectFrom!(DeleteRequest, CustomSpu);
ObjectFrom!(ListRequest, CustomSpu);
ObjectFrom!(WatchRequest, CustomSpu);
ObjectFrom!(WatchResponse, CustomSpu);

ObjectTryFrom!(WatchResponse, CustomSpu);
ObjectTryFrom!(ListResponse, CustomSpu);
