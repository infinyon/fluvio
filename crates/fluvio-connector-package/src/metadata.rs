use anyhow::anyhow;
use std::{collections::BTreeMap, ops::Deref, fmt::Display};

use fluvio_controlplane_metadata::smartmodule::FluvioSemVersion;
use serde::{Serialize, Deserialize};

use crate::config::ConnectorConfig;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ConnectorMetadata {
    pub package: ConnectorPackage,
    pub direction: Direction,
    pub deployment: Deployment,
    #[serde(rename = "secret", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: Secrets,
    #[serde(rename = "params", default, skip_serializing_if = "Vec::is_empty")]
    pub parameters: Parameters,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct ConnectorPackage {
    pub name: String,
    pub group: String,
    pub version: FluvioSemVersion,
    pub fluvio: FluvioSemVersion,
    #[serde(rename = "apiVersion")]
    pub api_version: FluvioSemVersion,
    pub description: Option<String>,
    pub license: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Direction {
    #[serde(default, skip_serializing_if = "is_false")]
    source: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    dest: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct Deployment {
    pub image: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct Parameters(Vec<Parameter>);

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct Parameter {
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub ty: ParameterType,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ParameterType {
    #[default]
    String,
    Integer,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct Secrets(BTreeMap<String, Secret>);

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct Secret {
    #[serde(rename = "type")]
    pub ty: SecretType,

    pub mount: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
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
            deployment: Deployment {
                image: "group/connector_image@0.0.0".into(),
            },
            package: ConnectorPackage {
                name: "NameOfConnector".into(),
                group: "GroupOfConnector".into(),
                version: FluvioSemVersion::parse("0.0.0").expect("invalid SemVer"),
                fluvio: FluvioSemVersion::parse("0.0.0").expect("invalid SemVer"),
                api_version: FluvioSemVersion::parse("0.0.0").expect("invalid SemVer"),
                description: Some("description text".into()),
                license: Some("e.g. Apache 2.0".into()),
            },
            parameters: Parameters::from(vec![Parameter {
                name: "param_name".into(),
                description: Some("description text".into()),
                ty: ParameterType::String,
            }]),
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
        write!(f, "{}", str)
    }
}

impl Deref for Parameters {
    type Target = Vec<Parameter>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for Secrets {
    type Target = BTreeMap<String, Secret>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<Parameter>> for Parameters {
    fn from(params: Vec<Parameter>) -> Self {
        Self(params)
    }
}

impl From<BTreeMap<String, Secret>> for Secrets {
    fn from(secrets: BTreeMap<String, Secret>) -> Self {
        Self(secrets)
    }
}

#[cfg(feature = "toml")]
impl ConnectorMetadata {
    pub fn from_toml_str(input: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(input)?)
    }

    pub fn from_toml_file<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read(path)?;
        Ok(toml::from_slice(content.as_slice())?)
    }

    pub fn to_toml_string(&self) -> anyhow::Result<String> {
        Ok(toml::to_string(self)?)
    }

    pub fn to_toml_file<P: AsRef<std::path::Path>>(&self, path: P) -> anyhow::Result<()> {
        let content = toml::to_vec(&self)?;
        std::fs::write(path, content.as_slice())?;
        Ok(())
    }
}

impl ConnectorMetadata {
    pub fn validate_config(&self, config: &ConnectorConfig) -> anyhow::Result<()> {
        validate_direction(&self.direction, config)?;
        validate_deployment(&self.deployment, config)?;
        validate_secrets(&self.secrets, config)?;
        validate_parameters(&self.parameters, config)?;
        Ok(())
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
    let cfg_image = config.image();
    if !deployment.image.eq(&cfg_image) {
        return Err(anyhow!(
            "deployment image in metadata: '{}' mismatches image in config: '{}'",
            &deployment.image,
            cfg_image
        ));
    }
    Ok(())
}

fn validate_secrets(secrets: &Secrets, config: &ConnectorConfig) -> anyhow::Result<()> {
    for meta_secret in secrets.keys() {
        if !config.secrets.contains_key(meta_secret) {
            return Err(anyhow!(
                "missing required secret '{}' in config",
                meta_secret
            ));
        }
    }
    for cfg_secret in config.secrets.keys() {
        if !secrets.contains_key(cfg_secret) {
            return Err(anyhow!(
                "config secret '{}' is not defined in package metadata",
                cfg_secret
            ));
        }
    }
    Ok(())
}

fn validate_parameters(parameters: &Parameters, config: &ConnectorConfig) -> anyhow::Result<()> {
    for meta_param in parameters.iter() {
        if !config.parameters.contains_key(&meta_param.name) {
            return Err(anyhow!(
                "missing required parameter '{}' in config",
                &meta_param.name
            ));
        }
    }
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

            [[params]]
            name = "int_param"
            description = "description"
            type = "integer"
        "#;

        //when
        let metadata = ConnectorMetadata::from_toml_str(toml_str).unwrap();

        //then
        assert_eq!(
            metadata,
            ConnectorMetadata {
                direction: Direction::dest(),
                deployment: Deployment {
                    image: "image_url".to_string()
                },
                package: ConnectorPackage {
                    name: "p_name".into(),
                    group: "p_group".into(),
                    version: FluvioSemVersion::parse("0.1.1").unwrap(),
                    fluvio: FluvioSemVersion::parse("0.1.2").unwrap(),
                    api_version: FluvioSemVersion::parse("0.1.3").unwrap(),
                    description: Some("descr".into()),
                    license: Some("license".into())
                },
                parameters: Parameters(vec![Parameter {
                    name: "int_param".into(),
                    description: Some("description".into()),
                    ty: ParameterType::Integer
                }]),
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
    use std::{collections::BTreeMap, str::FromStr};

    use crate::{
        metadata::{Secret, SecretType, Parameter, ParameterType},
        config::{SecretString, ConnectorParameterValue},
    };

    use super::*;

    #[test]
    fn test_validate_direction() {
        //given
        let source = Direction::source();
        let dest = Direction::dest();
        let source_config = ConnectorConfig {
            type_: "http-source".into(),
            ..Default::default()
        };
        let sink_config = ConnectorConfig {
            type_: "http-sink".into(),
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
            type_: "http_source".into(),
            version: "latest".into(),
            ..Default::default()
        };
        let deployment1 = Deployment {
            image: "infinyon/fluvio-connect-http_source:latest".into(),
        };
        let deployment2 = Deployment {
            image: "infinyon/fluvio-connect-http_sink:latest".into(),
        };

        //when
        validate_deployment(&deployment1, &config).unwrap();
        let res = validate_deployment(&deployment2, &config);

        //then
        assert_eq!(res.unwrap_err().to_string(), "deployment image in metadata: 'infinyon/fluvio-connect-http_sink:latest' mismatches image in config: 'infinyon/fluvio-connect-http_source:latest'");
    }

    #[test]
    fn test_validate_secrets_missing() {
        //given
        let config = ConnectorConfig::default();
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
        assert_eq!(
            res.unwrap_err().to_string(),
            "missing required secret 'secret_name' in config"
        );
    }

    #[test]
    fn test_validate_secrets_undefined() {
        //given
        let config = ConnectorConfig {
            secrets: BTreeMap::from([("secret_name".into(), SecretString::from_str("").unwrap())]),
            ..Default::default()
        };

        //when
        let res = validate_secrets(&Secrets::default(), &config);

        //then
        assert_eq!(
            res.unwrap_err().to_string(),
            "config secret 'secret_name' is not defined in package metadata"
        );
    }

    #[test]
    fn test_validate_parameters_missing() {
        //given
        let config = ConnectorConfig::default();
        let meta_params = Parameters::from(vec![Parameter {
            name: "param_name".into(),
            description: None,
            ty: ParameterType::String,
        }]);

        //when
        let res = validate_parameters(&meta_params, &config);

        //then
        assert_eq!(
            res.unwrap_err().to_string(),
            "missing required parameter 'param_name' in config"
        );
    }

    #[test]
    fn test_validate_config() {
        //given
        let config = ConnectorConfig {
            type_: "http-source".into(),
            version: "latest".into(),
            parameters: BTreeMap::from([(
                "param_name".into(),
                ConnectorParameterValue::from("param_value"),
            )]),
            secrets: BTreeMap::from([("secret_name".into(), SecretString::from_str("").unwrap())]),
            ..Default::default()
        };
        let metadata = ConnectorMetadata {
            direction: Direction::source(),
            deployment: Deployment {
                image: "infinyon/fluvio-connect-http-source:latest".into(),
            },
            secrets: Secrets::from(BTreeMap::from([(
                "secret_name".into(),
                Secret {
                    ty: SecretType::Env,
                    mount: None,
                },
            )])),
            parameters: Parameters::from(vec![Parameter {
                name: "param_name".into(),
                description: None,
                ty: ParameterType::String,
            }]),
            ..Default::default()
        };

        //when
        metadata.validate_config(&config).unwrap();

        //then
    }
}
