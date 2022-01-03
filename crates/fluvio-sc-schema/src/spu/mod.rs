pub use fluvio_controlplane_metadata::spu::{SpuSpec};

use crate::objects::ListRequest;
use crate::objects::ListResponse;
use crate::objects::Metadata;
use crate::{
    AdminSpec, NameFilter,
    objects::{ObjectFrom, ObjectTryFrom, WatchResponse, WatchRequest},
};

impl AdminSpec for SpuSpec {
    type ListFilter = NameFilter;
    type ListType = Metadata<Self>;
    type WatchResponseType = Self;
}

ObjectFrom!(WatchRequest, Spu);
ObjectFrom!(WatchResponse, Spu);

ObjectFrom!(ListRequest, Spu);
ObjectFrom!(ListResponse, Spu);

ObjectTryFrom!(WatchResponse, Spu);
ObjectTryFrom!(ListResponse, Spu);
