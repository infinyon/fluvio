pub use fluvio_controlplane_metadata::spu::{CustomSpuSpec, CustomSpuKey};

use crate::CreatableAdminSpec;
use crate::DeletableAdminSpec;
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
    type WatchResponseType = Self;
}

impl CreatableAdminSpec for CustomSpuSpec {
    const CREATE_TYPE: u8 = 1;
}

impl DeletableAdminSpec for CustomSpuSpec {
    type DeleteKey = CustomSpuKey;
}

ObjectFrom!(CreateRequest, CustomSpu);
ObjectFrom!(DeleteRequest, CustomSpu);
ObjectFrom!(ListRequest, CustomSpu);
ObjectFrom!(ListResponse, CustomSpu);
ObjectFrom!(WatchRequest, CustomSpu);
ObjectFrom!(WatchResponse, CustomSpu);

ObjectTryFrom!(WatchResponse, CustomSpu);
ObjectTryFrom!(ListResponse, CustomSpu);
