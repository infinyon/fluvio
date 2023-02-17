use serde::Deserialize;
use serde::Serialize;

use crate::k8_types::{Crd, GROUP, V1, CrdNames, Spec, Status, DefaultHeader};

use super::SmartModuleStatus;
use super::SmartModuleSpec;
use super::spec_v1::SmartModuleSpecV1;

const V2: &str = "v2";

const SMART_MODULE_V2_API: Crd = Crd {
    group: GROUP,
    version: V2,
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
        &SMART_MODULE_V2_API
    }
}

impl Status for SmartModuleStatus {}

const SMART_MODULE_V1_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "SmartModule",
        plural: "smartmodules",
        singular: "smartmodule",
    },
};

/// SmartModuleV1 could be empty
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SmartModuleV1Wrapper {
    #[serde(flatten)]
    pub inner: Option<SmartModuleSpecV1>,
}

impl Spec for SmartModuleV1Wrapper {
    type Status = SmartModuleStatus;
    type Header = DefaultHeader;

    fn metadata() -> &'static Crd {
        &SMART_MODULE_V1_API
    }
}

impl From<SmartModuleSpecV1> for SmartModuleSpec {
    fn from(v1: SmartModuleSpecV1) -> Self {
        Self {
            wasm: v1.wasm,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;

    use crate::smartmodule::spec_v1::SmartModuleInputKind;

    #[test]
    fn test_sm_spec_v1_simple() {
        use super::SmartModuleSpecV1;

        let yaml_spec: &str = r#"
input_kind: Stream
output_kind: Stream
wasm:
    format: BINARY
    payload: AGFzbQEAAAA=
"#;
        let sm_spec: SmartModuleSpecV1 =
            serde_yaml::from_str(yaml_spec).expect("Failed to deserialize");

        assert_eq!(sm_spec.input_kind, SmartModuleInputKind::Stream);
    }

    /// convert wasm code to base64,
    /// this is used by test_sm_spec_v1_simple
    #[test]
    fn test_encode_wasm_header() {
        let bytes: Vec<u8> = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        let encoded_string = base64::engine::general_purpose::STANDARD.encode(bytes);

        assert_eq!(encoded_string, "AGFzbQEAAAA=");
    }
}

#[cfg(test)]
mod test_v2_spec {

    use std::{io::BufReader, fs::File};

    use fluvio_stream_model::k8_types::K8Obj;
    use crate::smartmodule::FluvioSemVersion;

    use super::SmartModuleSpec;

    type K8SmartModuleSpec = K8Obj<SmartModuleSpec>;

    #[test]
    fn read_sm_from_k8() {
        let reader = BufReader::new(File::open("tests/sm_k8_v2.yaml").expect("v2 not found"));
        let sm_k8: K8SmartModuleSpec =
            serde_yaml::from_reader(reader).expect("failed to parse sm k8");

        let metadata = sm_k8.spec.meta.expect("metadata not found");
        assert_eq!(metadata.package.name, "module1");
        assert_eq!(
            metadata.package.version,
            FluvioSemVersion::parse("0.1.0").unwrap()
        );
        assert_eq!(
            metadata.package.description.unwrap(),
            "This is a test module"
        );
        assert_eq!(
            metadata.package.api_version,
            FluvioSemVersion::parse("0.1.0").unwrap()
        );

        let params = metadata.params;
        assert_eq!(params.len(), 2);
        let input1 = params.get_param("multipler").unwrap();
        assert_eq!(input1.description.as_ref().unwrap(), "multipler");
        assert!(!input1.optional);
        let input2 = params.get_param("scaler").unwrap();
        assert_eq!(input2.description.as_ref().unwrap(), "used for scaling");
        assert!(input2.optional);
    }
}
