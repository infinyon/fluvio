use crate::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::SmartStreamSpec;
use super::SmartStreamStatus;

const SMART_STREAM_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "SmartStream",
        plural: "smartstreams",
        singular: "smartstream",
    },
};

impl Spec for SmartStreamSpec {
    type Status = SmartStreamStatus;
    type Header = DefaultHeader;

    fn metadata() -> &'static Crd {
        &SMART_STREAM_API
    }
}

impl Status for SmartStreamStatus {}
