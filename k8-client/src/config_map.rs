use serde::Deserialize;
use serde::Serialize;

use k8_metadata::core::Crd;
use k8_metadata::core::CrdNames;
use k8_metadata::core::Spec;
use k8_metadata::core::Status;


//
// ConfigMap Object
const CONFIG_MAP_API: Crd = Crd {
    group: "core",
    version: "v1",
    names: CrdNames {
        kind: "ConfigMap",
        plural: "configmaps",
        singular: "configmap",
    },
};

impl Spec for ConfigMapSpec {

    type Status = ConfigMapStatus;

    fn metadata() -> &'static Crd {
        &CONFIG_MAP_API
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapSpec {}

#[derive(Deserialize, Serialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapStatus {}

impl Status for ConfigMapStatus{}