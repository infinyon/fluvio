use std::str::FromStr;

use fluvio::FluvioConfig;
use serde::{Serialize, Deserialize};

const INSTALLATION_METADATA_NAME: &str = "installation";
const CLOUD_METADATA_NAME: &str = "infinyon_cloud_cli";

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstallationType {
    #[default]
    K8,
    Local,
    LocalK8,
    ReadOnly,
    Docker,
    Cloud,
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
        if s.eq_ignore_ascii_case("docker") {
            return Ok(Self::Docker);
        }
        if s.eq_ignore_ascii_case("cloud") {
            return Ok(Self::Cloud);
        }
        Err(format!("unsupported installation type '{s}'"))
    }
}

impl std::fmt::Display for InstallationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl InstallationType {
    pub fn load(config: &FluvioConfig) -> Self {
        let install = config.query_metadata_by_name(INSTALLATION_METADATA_NAME);
        let cloud = config.has_metadata(CLOUD_METADATA_NAME);

        match (install, cloud) {
            (Some(install), _) => install,
            (None, true) => InstallationType::Cloud,
            (None, false) => InstallationType::K8,
        }
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
        assert_eq!(InstallationType::load(&config), InstallationType::K8);

        installation.save_to(&mut config).expect("saved");

        //then
        assert_eq!(InstallationType::load(&config), installation);
    }
}
