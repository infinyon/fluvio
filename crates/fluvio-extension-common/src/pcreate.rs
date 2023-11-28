use std::str::FromStr;

use serde::{Serialize, Deserialize};

use fluvio::FluvioConfig;

pub const PCREATE_METADATA_NAME: &str = "pcreate";

// profile creation metadata
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PCreateType {
    #[default]
    Local, // in this context, a self managed cluster of any installation_type config
    Cloud,
}

impl FromStr for PCreateType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("local") {
            return Ok(Self::Local);
        }
        if s.eq_ignore_ascii_case("local") {
            return Ok(Self::Cloud);
        }

        Err(format!("unsupported profile create metadata '{s}'"))
    }
}

impl std::fmt::Display for PCreateType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl PCreateType {
    pub fn load_or_default(config: &FluvioConfig) -> Self {
        config
            .query_metadata_by_name(PCREATE_METADATA_NAME)
            .unwrap_or_default()
    }

    pub fn save_to(&self, config: &mut FluvioConfig) -> anyhow::Result<()> {
        config.update_metadata_by_name(PCREATE_METADATA_NAME, self)
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

        //when
        assert_eq!(PCreateType::load_or_default(&config), PCreateType::Local);

        let pcreate = PCreateType::Cloud;
        pcreate.save_to(&mut config).expect("saved");

        //then
        assert_eq!(PCreateType::load_or_default(&config), pcreate);
    }
}
