//!
//! # Profile Configurations
//!
//! Stores configuration parameter retrieved from the default or custom profile file.
//!
use serde::{Serialize, Deserialize};
use toml::Table as Metadata;

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
    #[serde(default = "Metadata::new", skip_serializing_if = "Metadata::is_empty")]
    metadata: Metadata,

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
            metadata: Metadata::new(),
            client_id: None,
        }
    }

    /// Add TLS configuration for this cluster.
    pub fn with_tls(mut self, tls: impl Into<TlsPolicy>) -> Self {
        self.tls = tls.into();
        self
    }

    pub fn query_metadata_by_name<'de, T>(&self, name: &str) -> Option<T>
    where
        T: Deserialize<'de>,
    {
        let metadata = self.metadata.get(name)?;

        T::deserialize(metadata.clone()).ok()
    }

    pub fn update_metadata_by_name<S>(&mut self, name: &str, data: S) -> anyhow::Result<()>
    where
        S: Serialize,
    {
        use toml::{Value, map::Entry};

        match self.metadata.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(Value::try_from(data)?);
            }
            Entry::Occupied(mut entry) => {
                *entry.get_mut() = Value::try_from(data)?;
            }
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
    use crate::config::{Config, ConfigFile};

    #[test]
    fn test_get_metadata_path() {
        let toml = r#"version = "2"
[profile.local]
cluster = "local"

[cluster.local]
endpoint = "127.0.0.1:9003"

[cluster.local.metadata.custom]
name = "foo"
"#;
        let profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster("local").unwrap();

        #[derive(Deserialize, Debug, PartialEq)]
        struct Custom {
            name: String,
        }

        let custom: Option<Custom> = config.query_metadata_by_name("custom");

        assert_eq!(
            custom,
            Some(Custom {
                name: "foo".to_owned()
            })
        );
    }

    #[test]
    fn test_create_metadata() {
        let toml = r#"version = "2"
[profile.local]
cluster = "local"

[cluster.local]
endpoint = "127.0.0.1:9003"
"#;
        let mut profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster_mut("local").unwrap();

        #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
        struct Preference {
            connection: String,
        }

        let preference = Preference {
            connection: "wired".to_owned(),
        };

        config
            .update_metadata_by_name("preference", preference.clone())
            .expect("failed to add metadata");

        let metadata = config.query_metadata_by_name("preference").expect("");

        assert_eq!(preference, metadata);
    }

    #[test]
    fn test_update_old_metadata() {
        let toml = r#"version = "2"
[profile.local]
cluster = "local"

[cluster.local]
endpoint = "127.0.0.1:9003"

[cluster.local.metadata.installation]
type = "local"
"#;
        let mut profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster_mut("local").unwrap();

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Installation {
            #[serde(rename = "type")]
            typ: String,
        }

        let mut install = config
            .query_metadata_by_name::<Installation>("installation")
            .expect("message");

        assert_eq!(
            install,
            Installation {
                typ: "local".to_owned()
            }
        );

        install.typ = "cloud".to_owned();

        config
            .update_metadata_by_name("installation", install)
            .expect("failed to add metadata");

        let metadata = config
            .query_metadata_by_name::<Installation>("installation")
            .expect("could not get Installation metadata");

        assert_eq!("cloud", metadata.typ);
    }

    #[test]
    fn test_update_with_new_metadata() {
        let toml = r#"version = "2"
[profile.local]
cluster = "local"

[cluster.local]
endpoint = "127.0.0.1:9003"

[cluster.local.metadata.installation]
type = "local"
"#;
        let mut profile: Config = toml::de::from_str(toml).unwrap();
        let config = profile.cluster_mut("local").unwrap();

        #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
        struct Preference {
            connection: String,
        }

        let preference = Preference {
            connection: "wired".to_owned(),
        };

        config
            .update_metadata_by_name("preference", preference.clone())
            .expect("failed to add metadata");

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Installation {
            #[serde(rename = "type")]
            typ: String,
        }

        let installation: Installation = config
            .query_metadata_by_name("installation")
            .expect("could not get installation metadata");
        assert_eq!(installation.typ, "local");

        let preference: Preference = config
            .query_metadata_by_name("preference")
            .expect("could not get preference metadata");
        assert_eq!(preference.connection, "wired");
    }

    #[test]
    fn test_profile_with_metadata() {
        let config_file = ConfigFile::load(Some("test-data/profiles/config.toml".to_owned()))
            .expect("could not parse config file");
        let config = config_file.config();

        let cluster = config
            .cluster("extra")
            .expect("could not find `extra` cluster in test file");

        let table = toml::toml! {
            [deep.nesting.example]
            key = "custom field"

            [installation]
            type = "local"
        };

        assert_eq!(cluster.metadata, table);
    }

    #[test]
    fn test_save_updated_metadata() {
        #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
        struct Installation {
            #[serde(rename = "type")]
            typ: String,
        }

        let mut config_file =
            ConfigFile::load(Some("test-data/profiles/updatable_config.toml".to_owned()))
                .expect("could not parse config file");
        let config = config_file.mut_config();

        let cluster = config
            .cluster_mut("updated")
            .expect("could not find `updated` cluster in test file");

        let table: toml::Table = toml::toml! {
            [installation]
            type = "local"
        };
        assert_eq!(cluster.metadata, table);

        cluster
            .update_metadata_by_name(
                "installation",
                Installation {
                    typ: "cloud".to_owned(),
                },
            )
            .expect("should have updated key");

        let updated_table: toml::Table = toml::toml! {
            [installation]
            type = "cloud"
        };

        assert_eq!(cluster.metadata, updated_table.clone());

        config_file.save().expect("failed to save config file");

        let mut config_file =
            ConfigFile::load(Some("test-data/profiles/updatable_config.toml".to_owned()))
                .expect("could not parse config file");
        let config = config_file.mut_config();
        let cluster = config
            .cluster_mut("updated")
            .expect("could not find `updated` cluster in test file");
        assert_eq!(cluster.metadata, updated_table);

        cluster
            .update_metadata_by_name(
                "installation",
                Installation {
                    typ: "local".to_owned(),
                },
            )
            .expect("teardown: failed to set installation type back to local");

        config_file
            .save()
            .expect("teardown: failed to set installation type back to local");
    }
}
