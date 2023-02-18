pub use fluvio_controlplane_metadata::spu::{CustomSpuSpec, CustomSpuKey};

use crate::AdminSpec;
use crate::CreatableAdminSpec;
use crate::DeletableAdminSpec;

impl AdminSpec for CustomSpuSpec {}

impl CreatableAdminSpec for CustomSpuSpec {
    const CREATE_TYPE: u8 = 1;
}

impl DeletableAdminSpec for CustomSpuSpec {
    type DeleteKey = CustomSpuKey;
}

