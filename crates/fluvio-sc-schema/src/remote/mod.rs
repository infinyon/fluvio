pub use fluvio_controlplane_metadata::remote::*;

use crate::{AdminSpec, CreatableAdminSpec, DeletableAdminSpec};

impl AdminSpec for RemoteSpec {}

impl CreatableAdminSpec for RemoteSpec {}

impl DeletableAdminSpec for RemoteSpec {
    type DeleteKey = String;
}
