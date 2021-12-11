pub use fluvio_controlplane_metadata::spu::{CustomSpuSpec, CustomSpuKey};

use crate::CreatableAdminSpec;
use crate::DeletableAdminSpec;
use crate::objects::CreateFrom;
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

    fn convert_from<C: fluvio_controlplane_metadata::core::MetadataItem>(
        obj: &fluvio_controlplane_metadata::store::MetadataStoreObject<Self, C>,
    ) -> Self::ListType {
        obj.clone().into()
    }
}

impl CreatableAdminSpec for CustomSpuSpec {
    const CREATE_TYPE: u8 = 1;
}

impl DeletableAdminSpec for CustomSpuSpec {
    type DeleteKey = CustomSpuKey;
}

CreateFrom!(CustomSpuSpec, CustomSpu);
ObjectFrom!(DeleteRequest, CustomSpu);
ObjectFrom!(ListRequest, CustomSpu);
ObjectFrom!(ListResponse, CustomSpu);
ObjectFrom!(WatchRequest, CustomSpu);
ObjectFrom!(WatchResponse, CustomSpu);

ObjectTryFrom!(WatchResponse, CustomSpu);
ObjectTryFrom!(ListResponse, CustomSpu);
