//!
//! # Profile Configurations
//!
//! Stores configuration parameter retrieved from the default or custom profile file.
//!
use serde::{Serialize, Deserialize};

use crate::{config::TlsPolicy, FluvioError};

use super::ConfigFile;

/// Fluvio Cluster Target Configuration
/// This is part of profile
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FluvioConfig {
    /// The address to connect to the Fluvio cluster
    // TODO use a validated address type.
    // We don't want to have a "" address.
    #[serde(alias = "addr")]
    pub endpoint: String,

    #[serde(default)]
    pub use_spu_local_address: bool,

    /// The TLS policy to use when connecting to the cluster
    // If no TLS field is present in config file,
    // use the default of NoTls
    #[serde(default)]
    pub tls: TlsPolicy,

    /// Cluster custom metadata
    pub metadata: Option<toml::Value>,

    /// This is not part of profile and doesn't persist.
    /// It is purely to override client id when creating ClientConfig
    #[serde(skip)]
    pub client_id: Option<String>,
}

impl FluvioConfig {
    /// get current cluster config from default profile
    pub fn load() -> Result<Self, FluvioError> {
        let config_file = ConfigFile::load_default_or_new()?;
        let cluster_config = config_file.config().current_cluster()?;
        Ok(cluster_config.to_owned())
    }

    /// Create a new cluster configuration with no TLS.
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            endpoint: addr.into(),
            use_spu_local_address: false,
            tls: TlsPolicy::Disabled,
            metadata: None,
            client_id: None,
        }
    }

    /// Add TLS configuration for this cluster.
    pub fn with_tls(mut self, tls: impl Into<TlsPolicy>) -> Self {
        self.tls = tls.into();
        self
    }

    pub fn query_metadata_path<'de, T>(&self, path: &str) -> Option<T>
    where
        T: Deserialize<'de>,
    {
        let mut metadata = self.metadata.as_ref()?;

        let (path, key) = {
            let mut split = path.split(&['[', ']']);
            (split.next().unwrap_or(path), split.next())
        };

        for part in path.split('.') {
            if let toml::Value::Table(table) = metadata {
                metadata = table.get(part)?;
            }
        }

        if let Some(key) = key {
            metadata = metadata.as_table()?.get(key)?;
        }

        T::deserialize(metadata.clone()).ok()
    }

    pub fn update_metadata_path<S>(&mut self, path: &str, data: S) -> anyhow::Result<()>
    where
        S: Serialize,
    {
        use toml::{Value, map::Map};

        let (path, key) = {
            let mut split = path.split(&['[', ']']);
            (split.next().unwrap_or(path), split.next())
        };

        if let Some(mut metadata) = self.metadata.as_mut() {
            for part in path.split('.') {
                let Value::Table(table) = metadata else {
                    break;
                };
                let nested = table
                    .entry(part)
                    .or_insert_with(|| Value::Table(Map::new()));
                metadata = nested;
            }

            if let Some(key) = key {
                metadata = metadata
                    .as_table_mut()
                    .expect("metadata should be a table at this point")
                    .get_mut(key)
                    .ok_or_else(|| anyhow::anyhow!("key does not exist"))?;
            }

            *metadata = Value::try_from(data)?;
        } else {
            // insert new metadata
            if path.contains(|c| c == '.' || c == '[') {
                return Err(anyhow::anyhow!("not supported"));
            }

            let table = Map::from_iter([(path.to_string(), Value::try_from(data)?)]).into();
            self.metadata = Some(table);
        }

        Ok(())
    }
}

impl TryFrom<FluvioConfig> for fluvio_socket::ClientConfig {
    type Error = std::io::Error;
    fn try_from(config: FluvioConfig) -> Result<Self, Self::Error> {
        let connector = fluvio_future::net::DomainConnector::try_from(config.tls.clone())?;
        Ok(Self::new(
            &config.endpoint,
            connector,
            config.use_spu_local_address,
        ))
    }
}

#[cfg(test)]
mod test_metadata {
    use serde::{Deserialize, Serialize};
    use crate::config::Config;

    #[test]
    fn test_get_metadata_path() {
        let toml = r#"version = "2"
[profile.local]
cluster = "name"

[cluster.name]
endpoint = "127.0.0.1:9003"

[cluster.name.metadata.custom]
name = "foo"
"#;
        let profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster("name").unwrap();

        #[derive(Deserialize, Debug, PartialEq)]
        struct Custom {
            name: String,
        }

        let custom: Option<Custom> = config.query_metadata_path("custom");

        assert_eq!(
            custom,
            Some(Custom {
                name: "foo".to_owned()
            })
        );
    }

    #[test]
    fn test_query_specific_field_of_metadata() {
        let toml = r#"version = "2"
[profile.local]
cluster = "name"

[cluster.name]
endpoint = "127.0.0.1:9003"

[cluster.name.metadata.deep.nested]
name = "foo"
"#;
        let profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster("name").unwrap();

        let custom: Option<String> = config.query_metadata_path("deep.nested[name]");

        assert_eq!(custom, Some("foo".to_owned()));
    }

    #[test]
    fn test_create_metadata() {
        let toml = r#"version = "2"
[profile.local]
cluster = "name"

[cluster.name]
endpoint = "127.0.0.1:9003"
"#;
        let mut profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster_mut("name").unwrap();

        #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
        struct Preference {
            connection: String,
        }

        let preference = Preference {
            connection: "wired".to_owned(),
        };

        config
            .update_metadata_path("preference", preference.clone())
            .expect("failed to add metadata");

        let metadata = config.query_metadata_path("preference").expect("");

        assert_eq!(preference, metadata);
    }

    #[test]
    fn test_update_old_metadata() {
        let toml = r#"version = "2"
[profile.local]
cluster = "name"

[cluster.name]
endpoint = "127.0.0.1:9003"

[cluster.name.metadata.installation]
type = "local"
"#;
        let mut profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster_mut("name").unwrap();

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Installation {
            #[serde(rename = "type")]
            typ: String,
        }

        let mut install = config
            .query_metadata_path::<Installation>("installation")
            .expect("message");

        assert_eq!(
            install,
            Installation {
                typ: "local".to_owned()
            }
        );

        install.typ = "cloud".to_owned();

        config
            .update_metadata_path("installation", install)
            .expect("failed to add metadata");

        let metadata = config
            .query_metadata_path::<Installation>("installation")
            .expect("");

        assert_eq!("cloud", metadata.typ);
    }

    #[test]
    fn test_update_with_new_metadata() {
        let toml = r#"version = "2"
[profile.local]
cluster = "name"

[cluster.name]
endpoint = "127.0.0.1:9003"

[cluster.name.metadata.installation]
type = "local"
"#;
        let mut profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster_mut("name").unwrap();

        #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
        struct Preference {
            connection: String,
        }

        let preference = Preference {
            connection: "wired".to_owned(),
        };

        config
            .update_metadata_path("preference", preference.clone())
            .expect("failed to add metadata");

        let installation_type: String = config
            .query_metadata_path("installation.type")
            .expect("could not get installation metadata");
        assert_eq!(installation_type, "local");

        let preference_connection: String = config
            .query_metadata_path("preference.connection")
            .expect("could not get preference metadata");
        assert_eq!(preference_connection, "wired");
    }

    #[test]
    fn test_update_specific_field() {
        let toml = r#"version = "2"
[profile.local]
cluster = "name"

[cluster.name]
endpoint = "127.0.0.1:9003"

[cluster.name.metadata.installation]
type = "local"
"#;
        let mut profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster_mut("name").unwrap();

        config
            .update_metadata_path("installation[type]", "cloud")
            .expect("could not find installation type field");

        let installation_type: String = config
            .query_metadata_path("installation[type]")
            .expect("could not find installation type field");
        assert_eq!(installation_type, "cloud");
    }

    #[test]
    fn test_create_dynamic_nested_field_errors() {
        let toml = r#"version = "2"
[profile.local]
cluster = "name"

[cluster.name]
endpoint = "127.0.0.1:9003"
"#;
        let mut profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster_mut("name").unwrap();

        let update = config.update_metadata_path("deep.nested[field]", "value");

        assert!(update.is_err());
    }

    #[test]
    fn test_update_partial_existant_path_errors() {
        let toml = r#"version = "2"
[profile.local]
cluster = "name"

[cluster.name]
endpoint = "127.0.0.1:9003"

[cluster.name.deep.nested]
key = "value"
"#;
        let mut profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster_mut("name").unwrap();

        let update = config.update_metadata_path("deep.nonexistent[key]", "value");

        assert!(update.is_err());
    }

    #[test]
    fn test_update_path_with_no_key_errors() {
        let toml = r#"version = "2"
[profile.local]
cluster = "name"

[cluster.name]
endpoint = "127.0.0.1:9003"

[cluster.name.deep.nested]
key = "value"
"#;
        let mut profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster_mut("name").unwrap();

        let update = config.update_metadata_path("deep.nested[nonexistent]", "value");

        assert!(update.is_err());
    }
}
