use std::str::FromStr;

use fluvio::FluvioConfig;
use serde::{Serialize, Deserialize};

const INSTALLATION_METADATA_NAME: &str = "installation";

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstallationType {
    #[default]
    K8,
    Local,
    LocalK8,
    ReadOnly,
}

impl FromStr for InstallationType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("k8") {
            return Ok(Self::K8);
        }
        if s.eq_ignore_ascii_case("local") {
            return Ok(Self::Local);
        }
        if s.eq_ignore_ascii_case("local-k8") {
            return Ok(Self::LocalK8);
        }
        if s.eq_ignore_ascii_case("local_k8") {
            return Ok(Self::LocalK8);
        }
        if s.eq_ignore_ascii_case("localk8") {
            return Ok(Self::LocalK8);
        }
        if s.eq_ignore_ascii_case("read_only") {
            return Ok(Self::ReadOnly);
        }
        if s.eq_ignore_ascii_case("read-only") {
            return Ok(Self::ReadOnly);
        }
        if s.eq_ignore_ascii_case("readonly") {
            return Ok(Self::ReadOnly);
        }
        Err(format!("unsupported instalaltion type '{s}'"))
    }
}

impl std::fmt::Display for InstallationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl InstallationType {
    pub fn load_or_default(config: &FluvioConfig) -> Self {
        config
            .query_metadata_by_name(INSTALLATION_METADATA_NAME)
            .unwrap_or_default()
    }

    pub fn save_to(&self, config: &mut FluvioConfig) -> anyhow::Result<()> {
        config.update_metadata_by_name(INSTALLATION_METADATA_NAME, self)
    }
}

#[cfg(test)]
mod tests {
    use fluvio::FluvioConfig;

    use super::*;

    #[test]
    fn test_installation_in_config_load_and_update() {
        //given
        let mut config = FluvioConfig::new("test");
        let installation = InstallationType::Local;

        //when
        assert_eq!(
            InstallationType::load_or_default(&config),
            InstallationType::K8
        );

        installation.save_to(&mut config).expect("saved");

        //then
        assert_eq!(InstallationType::load_or_default(&config), installation);
    }
}
