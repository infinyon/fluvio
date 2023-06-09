use crate::k8_types::{Crd, GROUP, CrdNames, Spec, Status, DefaultHeader};

use super::SchemaStatus;
use super::SchemaSpec;

const SCHEMA_V1_API: Crd = Crd {
    group: GROUP,
    version: "v1",
    names: CrdNames {
        kind: "Schema",
        plural: "schemas",
        singular: "schema",
    },
};

impl Status for SchemaStatus {}

impl Spec for SchemaSpec {
    type Status = SchemaStatus;
    type Header = DefaultHeader;

    fn metadata() -> &'static Crd {
        &SCHEMA_V1_API
    }
}
