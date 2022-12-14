use std::collections::BTreeMap;

use fluvio_controlplane_metadata::smartmodule::FluvioSemVersion;
use fluvio_connector_package::metadata::*;

#[test]
fn test_read_from_toml_file() {
    //given
    let path = format!("{}/tests/Connector.toml", env!("CARGO_MANIFEST_DIR"));

    //when
    let metadata = ConnectorMetadata::from_toml_file(path).unwrap();

    //then
    assert_eq!(
        metadata,
        ConnectorMetadata {
            direction: Direction::source(),
            deployment: Deployment::from_binary_name("json-test-connector"),
            package: ConnectorPackage {
                name: "json-test-connector".into(),
                group: "fluvio".into(),
                version: FluvioSemVersion::parse("0.1.0").unwrap(),
                fluvio: FluvioSemVersion::parse("0.10.0").unwrap(),
                api_version: FluvioSemVersion::parse("0.1.0").unwrap(),
                description: Some("Generate JSON generator".into()),
                license: Some("Apache-2.0".into()),
                visibility: ConnectorVisibility::Public,
            },
            parameters: Parameters::from(vec![Parameter {
                name: "template".into(),
                description: Some("JSON template".into()),
                ty: ParameterType::String
            }]),
            secrets: Secrets::from(BTreeMap::from([
                (
                    "password".into(),
                    Secret {
                        ty: SecretType::Env,
                        mount: None,
                    }
                ),
                (
                    "my_cert".into(),
                    Secret {
                        ty: SecretType::File,
                        mount: Some("/mydata/secret1".into())
                    }
                )
            ])),
        }
    )
}

#[test]
fn test_write_to_toml_file() {
    //given
    let file = tempfile::NamedTempFile::new().unwrap();
    let path = format!("{}/tests/Connector.toml", env!("CARGO_MANIFEST_DIR"));

    let metadata = ConnectorMetadata::from_toml_file(path).unwrap();
    //when
    metadata.to_toml_file(file.as_ref()).unwrap();

    let content = std::fs::read_to_string(file).unwrap();
    println!("{}", &content);

    //then
    assert_eq!(
        content,
        r#"[package]
name = "json-test-connector"
group = "fluvio"
version = "0.1.0"
fluvio = "0.10.0"
apiVersion = "0.1.0"
description = "Generate JSON generator"
license = "Apache-2.0"
visibility = "public"

[direction]
source = true

[deployment]
binary = "json-test-connector"
[secret.my_cert]
type = "file"
mount = "/mydata/secret1"

[secret.password]
type = "env"

[[params]]
name = "template"
description = "JSON template"
type = "string"
"#
    );
}
