use fluvio_controlplane_metadata::smartmodule::FluvioSemVersion;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct ConnectorMetadata {
    pub direction: Direction,
    pub deployment: Deployment,
    pub package: ConnectorPackage,
    #[serde(
        rename = "params",
        default,
        skip_serializing_if = "Parameters::is_empty"
    )]
    pub parameters: Parameters,
}

#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq)]
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

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Direction {
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dest: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct Deployment {
    pub image: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct Parameters(Vec<Parameter>);

#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct Parameter {
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub ty: ParameterType,
}

#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ParameterType {
    #[default]
    String,
    Integer,
}

impl Direction {
    pub fn source() -> Self {
        Self {
            source: Some(true),
            dest: None,
        }
    }

    pub fn dest() -> Self {
        Self {
            source: None,
            dest: Some(true),
        }
    }
}

impl Default for Direction {
    fn default() -> Self {
        Self::source()
    }
}

impl Parameters {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<Parameter>> for Parameters {
    fn from(params: Vec<Parameter>) -> Self {
        Self(params)
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

#[cfg(feature = "toml")]
#[cfg(test)]
mod tests {
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
                }])
            }
        )
    }
}
