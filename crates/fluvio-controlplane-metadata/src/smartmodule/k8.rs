use crate::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::SmartModuleStatus;
use super::SmartModuleSpec;

const SMART_MODULE_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "SmartModule",
        plural: "smartmodules",
        singular: "smartmodule",
    },
};

impl Spec for SmartModuleSpec {
    type Status = SmartModuleStatus;
    type Header = DefaultHeader;

    fn metadata() -> &'static Crd {
        &SMART_MODULE_API
    }
}

impl Status for SmartModuleStatus {}
