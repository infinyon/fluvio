use fluvio_connector_package::metadata::Parameter;
use fluvio_connector_package::metadata::Parameters;
use fluvio_connector_package::metadata::ParameterType;
use fluvio_controlplane_metadata::smartmodule::FluvioSemVersion;
use fluvio_connector_package::metadata::ConnectorPackage;
use fluvio_connector_package::metadata::Deployment;
use fluvio_connector_package::metadata::Direction;
use fluvio_connector_package::metadata::ConnectorMetadata;

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
            deployment: Deployment {
                image: "fluvio/json-test-connector:0.1.0".to_string()
            },
            package: ConnectorPackage {
                name: "json-test-connector".into(),
                group: "fluvio".into(),
                version: FluvioSemVersion::parse("0.1.0").unwrap(),
                fluvio: FluvioSemVersion::parse("0.10.0").unwrap(),
                api_version: FluvioSemVersion::parse("0.1.0").unwrap(),
                description: Some("Generate JSON generator".into()),
                license: Some("Apache-2.0".into())
            },
            parameters: Parameters::from(vec![Parameter {
                name: "template".into(),
                description: Some("JSON template".into()),
                ty: ParameterType::String
            }])
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
        r#"[direction]
source = true

[deployment]
image = "fluvio/json-test-connector:0.1.0"

[package]
name = "json-test-connector"
group = "fluvio"
version = "0.1.0"
fluvio = "0.10.0"
api_version = "0.1.0"
description = "Generate JSON generator"
license = "Apache-2.0"

[[params]]
name = "template"
description = "JSON template"
type = "string"
"#
    );
}
