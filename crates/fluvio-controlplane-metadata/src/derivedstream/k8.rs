use crate::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::DerivedStreamSpec;
use super::DerivedStreamStatus;

const SMART_STREAM_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "DerivedStream",
        plural: "derivedstreams",
        singular: "derivedstream",
    },
};

impl Spec for DerivedStreamSpec {
    type Status = DerivedStreamStatus;
    type Header = DefaultHeader;

    fn metadata() -> &'static Crd {
        &SMART_STREAM_API
    }
}

impl Status for DerivedStreamStatus {}
