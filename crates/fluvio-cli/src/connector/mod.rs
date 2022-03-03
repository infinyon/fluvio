use std::sync::Arc;
use structopt::StructOpt;

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;

use fluvio::Fluvio;
use fluvio::metadata::connector::{ManagedConnectorSpec, ManagedConnectorMetadata, SecretString};
use fluvio_extension_common::Terminal;
use fluvio_extension_common::COMMAND_TEMPLATE;

mod create;
mod update;
mod delete;
mod list;
mod logs;

use create::CreateManagedConnectorOpt;
use update::UpdateManagedConnectorOpt;
use delete::DeleteManagedConnectorOpt;
use list::ListManagedConnectorsOpt;
use logs::LogsManagedConnectorOpt;
use crate::CliError;

#[derive(Debug, StructOpt)]
pub enum ManagedConnectorCmd {
    /// Create a new Managed Connector
    #[structopt(
        name = "create",
        template = COMMAND_TEMPLATE,
    )]
    Create(CreateManagedConnectorOpt),

    /// Update a Managed Connector
    #[structopt(
        name = "update",
        template = COMMAND_TEMPLATE,
    )]
    Update(UpdateManagedConnectorOpt),

    /// Delete a Managed Connector
    #[structopt(
        name = "delete",
        template = COMMAND_TEMPLATE,
    )]
    Delete(DeleteManagedConnectorOpt),

    /// Get the logs for a Managed Connector
    #[structopt(
        name = "logs",
        template = COMMAND_TEMPLATE,
    )]
    Logs(LogsManagedConnectorOpt),

    /// List all Managed Connectors
    #[structopt(
        name = "list",
        template = COMMAND_TEMPLATE,
    )]
    List(ListManagedConnectorsOpt),
}

impl ManagedConnectorCmd {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<(), CliError> {
        match self {
            Self::Create(create) => {
                create.process(fluvio).await?;
            }
            Self::Update(update) => {
                update.process(fluvio).await?;
            }
            Self::Delete(delete) => {
                delete.process(fluvio).await?;
            }
            Self::Logs(logs) => {
                logs.process().await?;
            }
            Self::List(list) => {
                list.process(out, fluvio).await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectorConfig {
    name: String,

    /// This field is similar to a github actions
    /// file:/// specifies a path to the metadata
    /// https:// specifies the url to the metadata
    /// github:// a github repo
    /// infinyon:http -> github.com/infinyon/fluvio-connectors/.../http/
    #[serde(deserialize_with = "string_or_struct")]
    uses: ManagedConnectorMetadata,

    pub(crate) topic: String,
    pub(crate) version: Option<String>,
    #[serde(default)]
    parameters: BTreeMap<String, String>,
    #[serde(default)]
    secrets: BTreeMap<String, SecretString>,
}

impl From<ConnectorConfig> for ManagedConnectorSpec {
    fn from(config: ConnectorConfig) -> ManagedConnectorSpec {
        ManagedConnectorSpec {
            name: config.name,
            topic: config.topic,
            parameters: config.parameters,
            secrets: config.secrets,
            version: config.version,
            metadata: Default::default(),
        }
    }
}

impl ConnectorConfig {
    pub async fn from_file<P: Into<PathBuf>>(path: P) -> Result<Self, CliError> {
        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let connector_config: Self = serde_yaml::from_str(&contents)?;
        Ok(connector_config)
    }

    /*
    pub async fn uses_to_metadata(&self) -> Result<ManagedConnectorMetadata, CliError> {
        let uses = &self.uses;
        if uses.starts_with("https://") {
            let body = surf::get(uses).recv_string().await?;
            Ok(serde_yaml::from_str(&body)?)
        } else if uses.starts_with("file://") {
            let path = uses
                .strip_prefix("file://")
                .ok_or_else(|| CliError::Other("Incorrectly formed file path".to_string()))?;
            println!("Opening file {:?}", path);
            let mut file = File::open(path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            let metadata: ManagedConnectorMetadata = serde_yaml::from_str(&contents)?;
            Ok(metadata)
        } else if uses.starts_with("github://") {
            todo!()
        } else if uses.starts_with("infinyon:") {
            let connector_type = uses
                .strip_prefix("infinyon:")
                .ok_or_else(|| CliError::Other("Incorrectly formed image name".to_string()))?;
            Ok(ManagedConnectorMetadata {
                image: format!("infinyon/fluvio-connect-{}", connector_type),
                author: Some("Fluvio Contributors <team@fluvio.io>".to_string()),
                license: Some("Apache-2.0".to_string()),
            })
        } else {
            Err(CliError::Other("No valid uses scheme supplied".to_string()))
        }
    }
    pub async fn to_managed_connector_spec(self) -> Result<ManagedConnectorSpec, CliError> {
        let metadata: ManagedConnectorMetadata = self.uses_to_metadata().await?;
        Ok(ManagedConnectorSpec {
            name: self.name,
            topic: self.topic,
            metadata,
            parameters: self.parameters,
            secrets: self.secrets,
            version: self.version,
        })
    }
    */
}

#[fluvio_future::test_async]
async fn simple_config_test() -> Result<(), ()> {
    let _: ManagedConnectorSpec =
        ConnectorConfig::from_file("test-data/connectors/simple-config.yaml").await
            .expect("Failed to load test config")
            .to_managed_connector_spec()
            .await
            .expect("Failed to load metadat");
    Ok(())
}

#[fluvio_future::test_async]
async fn file_metadata_config_test() -> Result<(), ()> {
    let _: ManagedConnectorSpec =
        ConnectorConfig::from_file("test-data/connectors/file-metadata-config.yaml").await
            .expect("Failed to load test config")
            .to_managed_connector_spec()
            .await
            .expect("Failed to load metadat");
    Ok(())
}

use std::collections::BTreeMap as Map;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;
use serde::de::{self, Visitor, MapAccess, Deserializer};

fn string_or_struct<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
	T: Deserialize<'de> + FromStr<Err = String>,
	D: Deserializer<'de>,
{
	struct StringOrStruct<T>(PhantomData<fn() -> T>);

	impl<'de, T> Visitor<'de> for StringOrStruct<T>
	where
		T: Deserialize<'de> + FromStr<Err = String>,
	{
		type Value = T;

		fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
			formatter.write_str("string or map")
		}

		fn visit_str<E>(self, value: &str) -> Result<T, E>
		where
			E: de::Error,
		{
			Ok(FromStr::from_str(value).unwrap())
		}

		fn visit_map<M>(self, map: M) -> Result<T, M::Error>
		where
			M: MapAccess<'de>,
		{
			// `MapAccessDeserializer` is a wrapper that turns a `MapAccess`
			// into a `Deserializer`, allowing it to be used as the input to T's
			// `Deserialize` implementation. T then deserializes itself using
			// the entries from the map visitor.
			Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
		}
	}

	deserializer.deserialize_any(StringOrStruct(PhantomData))
}
