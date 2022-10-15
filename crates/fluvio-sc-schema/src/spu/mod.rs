pub use fluvio_controlplane_metadata::spu::{SpuSpec};

use crate::objects::ListRequest;
use crate::objects::ListResponse;
use crate::{
    AdminSpec, NameFilter,
    objects::{ObjectFrom, ObjectTryFrom, WatchResponse, WatchRequest},
};

impl AdminSpec for SpuSpec {
    type ListFilter = NameFilter;
}

ObjectFrom!(WatchRequest, Spu);
ObjectFrom!(WatchResponse, Spu);

ObjectFrom!(ListRequest, Spu);
ObjectFrom!(ListResponse, Spu);

ObjectTryFrom!(WatchResponse, Spu);
ObjectTryFrom!(ListResponse, Spu);
