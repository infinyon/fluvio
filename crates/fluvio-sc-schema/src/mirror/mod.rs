pub use fluvio_controlplane_metadata::mirror::*;

use crate::{AdminSpec, CreatableAdminSpec, DeletableAdminSpec};

impl AdminSpec for MirrorSpec {}

impl CreatableAdminSpec for MirrorSpec {}

impl DeletableAdminSpec for MirrorSpec {
    type DeleteKey = String;
}
