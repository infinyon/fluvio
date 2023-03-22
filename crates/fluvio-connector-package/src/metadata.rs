use std::{collections::BTreeMap, ops::Deref, fmt::Display};

use anyhow::{anyhow, Context};
use openapiv3::{Schema, AnySchema, ReferenceOr, Type, SchemaKind};
use serde::{Serialize, Deserialize};

use fluvio_controlplane_metadata::smartmodule::FluvioSemVersion;

use crate::{config::ConnectorConfig, secret::detect_secrets};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectorMetadata {
    pub package: ConnectorPackage,
    pub direction: Direction,
    pub deployment: Deployment,
    #[serde(rename = "secret", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: Secrets,
    #[serde(rename = "custom", default, skip_serializing_if = "Option::is_none")]
    pub custom_config: CustomConfigSchema,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ConnectorPackage {
    pub name: String,
    pub group: String,
    pub version: FluvioSemVersion,
    pub fluvio: FluvioSemVersion,
    #[serde(rename = "apiVersion")]
    pub api_version: FluvioSemVersion,
    pub description: Option<String>,
    pub license: Option<String>,
    #[serde(default)]
    pub visibility: ConnectorVisibility,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorVisibility {
    #[default]
    Private,
    Public,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Direction {
    #[serde(default, skip_serializing_if = "is_false")]
    source: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    dest: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Deployment {
    pub image: Option<String>,
    pub binary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct CustomConfigSchema {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(flatten)]
    pub schema: Option<openapiv3::Schema>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Secrets(BTreeMap<String, Secret>);

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Secret {
    #[serde(rename = "type")]
    pub ty: SecretType,

    pub mount: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SecretType {
    #[default]
    Env,
    File,
}

impl Default for ConnectorMetadata {
    fn default() -> Self {
        Self {
            direction: Direction::source(),
            deployment: Deployment::from_image_name("group/connector_image@0.0.0"),
            package: ConnectorPackage {
                name: "NameOfConnector".into(),
                group: "GroupOfConnector".into(),
                version: FluvioSemVersion::parse("0.0.0").expect("invalid SemVer"),
                fluvio: FluvioSemVersion::parse("0.0.0").expect("invalid SemVer"),
                api_version: FluvioSemVersion::parse("0.0.0").expect("invalid SemVer"),
                description: Some("description text".into()),
                license: Some("e.g. Apache 2.0".into()),
                visibility: Default::default(),
            },
            custom_config: CustomConfigSchema::new(
                [("prop1", Type::String(Default::default()))],
                [],
            ),
            secrets: Secrets::from(BTreeMap::from([(
                "secret_name".into(),
                Secret {
                    ty: SecretType::Env,
                    mount: None,
                },
            )])),
        }
    }
}

impl Direction {
    pub fn source() -> Self {
        Self {
            source: true,
            dest: false,
        }
    }

    pub fn dest() -> Self {
        Self {
            source: false,
            dest: true,
        }
    }

    pub fn is_source(&self) -> bool {
        self.source
    }
}

impl Default for Direction {
    fn default() -> Self {
        Self::source()
    }
}

impl Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = if self.source { "source" } else { "dest" };
        write!(f, "{str}")
    }
}

impl Deref for Secrets {
    type Target = BTreeMap<String, Secret>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for CustomConfigSchema {
    type Target = Option<openapiv3::Schema>;

    fn deref(&self) -> &Self::Target {
        &self.schema
    }
}

impl From<BTreeMap<String, Secret>> for Secrets {
    fn from(secrets: BTreeMap<String, Secret>) -> Self {
        Self(secrets)
    }
}

impl Deployment {
    pub fn from_image_name<T: Into<String>>(image: T) -> Self {
        Self {
            image: Some(image.into()),
            binary: None,
        }
    }

    pub fn from_binary_name<T: Into<String>>(binary: T) -> Self {
        Self {
            image: None,
            binary: Some(binary.into()),
        }
    }
}

impl CustomConfigSchema {
    pub fn new<S, P, R>(properties: P, required: R) -> Self
    where
        S: Into<String>,
        P: IntoIterator<Item = (S, Type)>,
        R: IntoIterator<Item = S>,
    {
        Self::with(
            properties.into_iter().map(|(n, t)| {
                (
                    n,
                    Schema {
                        schema_data: Default::default(),
                        schema_kind: SchemaKind::Type(t),
                    },
                )
            }),
            required,
        )
    }

    pub fn with<S, P, R>(properties: P, required: R) -> Self
    where
        S: Into<String>,
        P: IntoIterator<Item = (S, Schema)>,
        R: IntoIterator<Item = S>,
    {
        let schema = Schema {
            schema_data: Default::default(),
            schema_kind: SchemaKind::Any(AnySchema {
                properties: FromIterator::from_iter(
                    properties
                        .into_iter()
                        .map(|(n, schema)| (n.into(), ReferenceOr::Item(Box::new(schema)))),
                ),
                required: required.into_iter().map(Into::into).collect(),
                ..Default::default()
            }),
        };
        Self {
            name: None,
            schema: Some(schema),
        }
    }
}

impl From<Schema> for CustomConfigSchema {
    fn from(value: Schema) -> Self {
        Self {
            name: None,
            schema: Some(value),
        }
    }
}

#[cfg(feature = "toml")]
impl ConnectorMetadata {
    pub fn from_toml_str(input: &str) -> anyhow::Result<Self> {
        toml::from_str(input).map_err(|err| anyhow::anyhow!(err))
    }

    pub fn from_toml_slice(input: &[u8]) -> anyhow::Result<Self> {
        toml::from_str(std::str::from_utf8(input)?).map_err(|err| anyhow::anyhow!(err))
    }

    pub fn from_toml_file<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read(path)?;
        Self::from_toml_slice(&content)
    }

    pub fn to_toml_string(&self) -> anyhow::Result<String> {
        let value = toml::Value::try_from(self)?; // serializing to Value first helps with ValueAfterTable error
        Ok(toml::to_string(&value)?)
    }

    pub fn to_toml_file<P: AsRef<std::path::Path>>(&self, path: P) -> anyhow::Result<()> {
        std::fs::write(path, self.to_toml_string()?)?;
        Ok(())
    }
}

impl ConnectorMetadata {
    pub fn validate_config<R: std::io::Read>(&self, reader: R) -> anyhow::Result<ConnectorConfig> {
        let value =
            serde_yaml::from_reader(reader).context("unable to parse config into YAML format")?;
        validate_custom_config(&self.custom_config, &value)
            .context("custom config validation failed")?;
        validate_secrets(&self.secrets, &value)?;
        let config = ConnectorConfig::from_value(value)
            .context("unable to parse common connector config")?;
        validate_direction(&self.direction, &config)?;
        validate_deployment(&self.deployment, &config)?;
        Ok(config)
    }
}

fn validate_direction(meta_direction: &Direction, config: &ConnectorConfig) -> anyhow::Result<()> {
    let cfg_direction = config.direction();
    if !cfg_direction.eq(meta_direction) {
        return Err(anyhow!(
            "direction in metadata: '{}' does not correspond direction in config: '{}'",
            meta_direction,
            cfg_direction
        ));
    }
    Ok(())
}

fn validate_deployment(deployment: &Deployment, config: &ConnectorConfig) -> anyhow::Result<()> {
    match (&deployment.image, &deployment.binary) {
        (None, None) => anyhow::bail!("deployment in metadata is not specified"),
        (None, Some(_)) => {}
        (Some(deployment_image), None) => {
            let cfg_image = config.image();
            if !deployment_image.eq(&cfg_image) {
                anyhow::bail!(
                    "deployment image in metadata: '{}' mismatches image in config: '{}'",
                    &deployment_image,
                    cfg_image
                );
            }
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("deployment contains both 'image' and 'binary' section")
        }
    };

    Ok(())
}

fn validate_secrets(secrets: &Secrets, config: &serde_yaml::Value) -> anyhow::Result<()> {
    let cfg_secrets = detect_secrets(config);
    for cfg_secret in cfg_secrets {
        if !secrets.contains_key(cfg_secret) {
            return Err(anyhow!(
                "config secret '{}' is not defined in package metadata",
                cfg_secret
            ));
        }
    }
    Ok(())
}

fn validate_custom_config(
    config_schema: &CustomConfigSchema,
    config: &serde_yaml::Value,
) -> anyhow::Result<()> {
    if let Some(schema) = config_schema.deref() {
        validate_schema_object(
            &schema.schema_kind,
            config
                .get(config_schema.name.as_deref().unwrap_or("custom"))
                .unwrap_or(&serde_yaml::Value::Mapping(Default::default())),
        )?;
    }
    Ok(())
}

fn validate_schema_object(kind: &SchemaKind, value: &serde_yaml::Value) -> anyhow::Result<()> {
    let (properties, required) = match kind {
        SchemaKind::Type(Type::Object(object)) => (&object.properties, object.required.as_slice()),
        SchemaKind::Any(schema) => (&schema.properties, schema.required.as_slice()),
        _ => return Ok(()),
    };

    if required.is_empty() {
        return Ok(());
    }

    if let serde_yaml::Value::Mapping(map) = value {
        for required_prop in required {
            match (
                properties.get(required_prop),
                map.get(&serde_yaml::Value::String(required_prop.clone())),
            ) {
                (None, _) => anyhow::bail!("required property is missing in the config schema"),
                (Some(ReferenceOr::Item(present_schema)), Some(present)) => {
                    validate_schema_object(&present_schema.as_ref().schema_kind, present)
                        .context(format!("validation failed for object '{required_prop}'"))?
                }
                (_, None) => anyhow::bail!(
                    "required property '{}' is not found in the config",
                    required_prop
                ),
                _ => {}
            }
        }
    } else {
        anyhow::bail!(
            "required config properties '{}' are not found in the config file",
            required.join(",")
        )
    };
    Ok(())
}

fn is_false(value: &bool) -> bool {
    !(*value)
}

#[cfg(feature = "toml")]
#[cfg(test)]
mod tests_toml {
    use super::*;

    #[test]
    fn test_empty_from_and_to_toml_string() {
        //given
        let metadata = ConnectorMetadata::default();

        //when
        let toml_string = metadata.to_toml_string().unwrap();
        let from_string = ConnectorMetadata::from_toml_str(toml_string.as_str()).unwrap();

        //then
        assert_eq!(metadata, from_string);
    }

    #[test]
    fn test_from_toml_str() {
        //given
        let toml_str = r#"
            [direction]
            dest = true

            [deployment]
            image = "image_url"

            [secret.password]
            type = "env"

            [secret.my_cert]
            type = "file"
            mount = "/mydata/secret1"

            [package]
            name = "p_name"
            group = "p_group"
            version = "0.1.1"
            fluvio = "0.1.2"
            apiVersion = "0.1.3"
            description = "descr"
            license = "license"
            visibility = "public"

            [custom]
            required = ["prop1"]

            [custom.properties.prop1]
            type = "integer"
            
            [custom.properties.prop2]
            type = "string"
        "#;

        //when
        let metadata = ConnectorMetadata::from_toml_str(toml_str).unwrap();

        //then
        assert_eq!(
            metadata,
            ConnectorMetadata {
                direction: Direction::dest(),
                deployment: Deployment::from_image_name("image_url"),
                package: ConnectorPackage {
                    name: "p_name".into(),
                    group: "p_group".into(),
                    version: FluvioSemVersion::parse("0.1.1").unwrap(),
                    fluvio: FluvioSemVersion::parse("0.1.2").unwrap(),
                    api_version: FluvioSemVersion::parse("0.1.3").unwrap(),
                    description: Some("descr".into()),
                    license: Some("license".into()),
                    visibility: ConnectorVisibility::Public,
                },
                custom_config: CustomConfigSchema::new(
                    [
                        ("prop1", Type::Integer(Default::default())),
                        ("prop2", Type::String(Default::default()))
                    ],
                    ["prop1"]
                ),
                secrets: Secrets(BTreeMap::from([
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
                ]))
            }
        )
    }
}
#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, io::Cursor};

    use openapiv3::ObjectType;

    use crate::{
        metadata::{Secret, SecretType},
        config::MetaConfig,
    };

    use super::*;

    #[test]
    fn test_validate_direction() {
        //given
        let source = Direction::source();
        let dest = Direction::dest();
        let source_config = ConnectorConfig {
            meta: MetaConfig {
                type_: "http-source".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let sink_config = ConnectorConfig {
            meta: MetaConfig {
                type_: "http-sink".into(),
                ..Default::default()
            },
            ..Default::default()
        };

        //when
        validate_direction(&source, &source_config).unwrap();
        validate_direction(&dest, &sink_config).unwrap();
        let res1 = validate_direction(&source, &sink_config);
        let res2 = validate_direction(&dest, &source_config);

        //then
        assert_eq!(
            res1.unwrap_err().to_string(),
            "direction in metadata: 'source' does not correspond direction in config: 'dest'"
        );
        assert_eq!(
            res2.unwrap_err().to_string(),
            "direction in metadata: 'dest' does not correspond direction in config: 'source'"
        );
    }

    #[test]
    fn test_validate_deployment() {
        //given
        let config = ConnectorConfig {
            meta: MetaConfig {
                type_: "http_source".into(),
                version: "latest".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let deployment1 = Deployment::from_image_name("infinyon/fluvio-connect-http_source:latest");
        let deployment2 = Deployment::from_image_name("infinyon/fluvio-connect-http_sink:latest");
        let deployment3 = Deployment::from_binary_name("http_sink_bin");

        //when
        validate_deployment(&deployment1, &config).unwrap();
        let res = validate_deployment(&deployment2, &config);
        validate_deployment(&deployment3, &config).unwrap();

        //then
        assert_eq!(res.unwrap_err().to_string(), "deployment image in metadata: 'infinyon/fluvio-connect-http_sink:latest' mismatches image in config: 'infinyon/fluvio-connect-http_source:latest'");
    }

    #[test]
    fn test_validate_secrets_missing() {
        //given
        let config = Default::default();
        let meta_secrets = Secrets::from(BTreeMap::from([(
            "secret_name".into(),
            Secret {
                ty: SecretType::Env,
                mount: None,
            },
        )]));

        //when
        let res = validate_secrets(&meta_secrets, &config);

        //then
        assert!(res.is_ok()); // config file is not obligated to use all secrets defined in
                              // Connector.toml
    }

    #[test]
    fn test_validate_secrets_undefined() {
        //given
        let config = serde_yaml::from_str("config:\n  secret:\n    name: secret_name").unwrap();
        //when
        let res = validate_secrets(&Secrets::default(), &config);

        //then
        assert_eq!(
            res.unwrap_err().to_string(),
            "config secret 'secret_name' is not defined in package metadata"
        );
    }

    #[test]
    fn test_validate_custom_config_empty() {
        //given
        let config = serde_yaml::from_str(
            r#"
                                          custom:
                                            key: value
                                          "#,
        )
        .unwrap();
        let config_schema = CustomConfigSchema::default();

        //when
        let res = validate_custom_config(&config_schema, &config);

        //then
        assert!(res.is_ok());
    }

    #[test]
    fn test_validate_custom_config_no_required_fields() {
        //given
        let config = serde_yaml::from_str(
            r#"
                                          custom:
                                            key: value
                                          "#,
        )
        .unwrap();
        let config_schema =
            CustomConfigSchema::new([("prop1", Type::Integer(Default::default()))], []);

        //when
        let res = validate_custom_config(&config_schema, &config);

        //then
        assert!(res.is_ok());
    }

    #[test]
    fn test_validate_custom_config_with_required_fields_missing() {
        //given
        let config = serde_yaml::from_str(
            r#"
                                          custom:
                                            key: value
                                          "#,
        )
        .unwrap();
        let config_schema =
            CustomConfigSchema::new([("prop1", Type::Integer(Default::default()))], ["prop1"]);

        //when
        let res = validate_custom_config(&config_schema, &config);

        //then
        assert_eq!(
            res.unwrap_err().to_string(),
            "required property 'prop1' is not found in the config"
        );
    }

    #[test]
    fn test_validate_custom_config_required_field_missing_in_schema() {
        //given
        let config = serde_yaml::from_str(
            r#"
                                        custom:
                                          prop2: value"#,
        )
        .unwrap();
        let config_schema =
            CustomConfigSchema::new([("prop1", Type::Integer(Default::default()))], ["prop2"]);

        //when
        let res = validate_custom_config(&config_schema, &config);

        //then
        assert_eq!(
            res.unwrap_err().to_string(),
            "required property is missing in the config schema"
        );
    }

    #[test]
    fn test_validate_custom_config_all_required_fields_missing() {
        //given
        let config = serde_yaml::from_str("custom:").unwrap();
        let config_schema = CustomConfigSchema::new(
            [
                ("prop1", Type::Integer(Default::default())),
                ("prop2", Type::Integer(Default::default())),
            ],
            ["prop1", "prop2"],
        );

        //when
        let res = validate_custom_config(&config_schema, &config);

        //then
        assert_eq!(
            res.unwrap_err().to_string(),
            "required config properties 'prop1,prop2' are not found in the config file"
        );
    }

    #[test]
    fn test_validate_custom_config_with_required_fields_present() {
        //given
        let config = serde_yaml::from_str(
            r#"
                custom:
                    key: value
                    prop1: 1
                "#,
        )
        .unwrap();
        let config_schema =
            CustomConfigSchema::new([("prop1", Type::Integer(Default::default()))], ["prop1"]);

        //when
        let res = validate_custom_config(&config_schema, &config);

        //then
        assert!(res.is_ok());
    }

    #[test]
    fn test_validate_custom_config_with_required_fields_present_overriden_name() {
        //given
        let config = serde_yaml::from_str(
            r#"
                json:
                    key: value
                    prop1: 1
                "#,
        )
        .unwrap();
        let mut config_schema =
            CustomConfigSchema::new([("prop1", Type::Integer(Default::default()))], ["prop1"]);
        config_schema.name = Some("json".into());

        //when
        let res = validate_custom_config(&config_schema, &config);

        //then
        assert!(res.is_ok());
    }

    #[test]
    fn test_validate_custom_config_with_complex_required_fields_present() {
        //given
        let config = serde_yaml::from_str(
            r#"
                custom:
                    key: value
                    prop1:
                        prop2: value
                "#,
        )
        .unwrap();

        let object_type = ObjectType {
            properties: [(
                "prop2".to_owned(),
                ReferenceOr::Item(Box::new(Schema {
                    schema_data: Default::default(),
                    schema_kind: SchemaKind::Type(Type::Integer(Default::default())),
                })),
            )]
            .into(),
            required: vec!["prop2".to_owned()],
            ..Default::default()
        };
        let config_schema =
            CustomConfigSchema::new([("prop1", Type::Object(object_type))], ["prop1"]);

        //when
        let res = validate_custom_config(&config_schema, &config);

        //then
        assert!(res.is_ok());
    }

    #[test]
    fn test_validate_custom_config_with_complex_required_fields_missing() {
        //given
        let config = serde_yaml::from_str(
            r#"
                custom:
                    key: value
                    prop1:
                        another: value
                "#,
        )
        .unwrap();

        let object_type = ObjectType {
            properties: [(
                "prop2".to_owned(),
                ReferenceOr::Item(Box::new(Schema {
                    schema_data: Default::default(),
                    schema_kind: SchemaKind::Type(Type::Integer(Default::default())),
                })),
            )]
            .into(),
            required: vec!["prop2".to_owned()],
            ..Default::default()
        };
        let config_schema =
            CustomConfigSchema::new([("prop1", Type::Object(object_type))], ["prop1"]);

        //when
        let res = validate_custom_config(&config_schema, &config);

        //then
        assert_eq!(
            res.unwrap_err().to_string(),
            "validation failed for object 'prop1'"
        );
    }

    #[test]
    fn test_validate_config() {
        //given
        let config = r#"
                meta:
                    name: my-http-source
                    topic: test-topic
                    type: http-source
                    version: latest
                    param-name: param-value
                custom:
                    prop1: 1
                    api_key:
                      secret:
                        name: secret_name
                "#;

        let metadata = ConnectorMetadata {
            direction: Direction::source(),
            deployment: Deployment::from_image_name("infinyon/fluvio-connect-http-source:latest"),
            secrets: Secrets::from(BTreeMap::from([(
                "secret_name".into(),
                Secret {
                    ty: SecretType::Env,
                    mount: None,
                },
            )])),
            custom_config: CustomConfigSchema::new(
                [("prop1", Type::Integer(Default::default()))],
                ["prop1"],
            ),
            ..Default::default()
        };

        //when
        let res = metadata.validate_config(Cursor::new(config.as_bytes()));

        //then
        assert!(res.is_ok());
    }
}
