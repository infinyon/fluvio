pub use fluvio_controlplane_metadata::remote_cluster::RemoteClusterSpec;

use crate::{AdminSpec, CreatableAdminSpec, DeletableAdminSpec};

impl AdminSpec for RemoteClusterSpec {}

impl CreatableAdminSpec for RemoteClusterSpec {}

impl DeletableAdminSpec for RemoteClusterSpec {
    type DeleteKey = String;
}
