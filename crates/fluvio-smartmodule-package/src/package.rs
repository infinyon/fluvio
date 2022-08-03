use std::collections::HashMap;
use std::fs::read_to_string;
use std::path::{Path};
use std::io::Result;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct SmartModuleMetadata {
    pub package: Package,
    pub init: HashMap<String, InitParam>,
}

impl SmartModuleMetadata {
    /// parse the metadata file and return the metadata
    pub fn from_file<T: AsRef<Path>>(path: T) -> Result<Self> {
        let path_ref = path.as_ref();
        let file_str: String = read_to_string(path_ref)?;
        let metadata = toml::from_str(&file_str)?;
        Ok(metadata)
    }
}

/// SmartModule Package metadata
#[derive(Serialize, Deserialize, Debug)]
pub struct Package {
    pub name: String,
    pub group: String,
    pub description: String,
    #[serde(default)]
    pub authors: Vec<String>,
    pub license: String,
    pub repository: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum InitType {
    String,
}

/// SmartModule initialization parameters
#[derive(Serialize, Deserialize, Debug)]
pub struct InitParam {
    // input_type, it should be enum
    pub input: InitType,
}

#[cfg(test)]
mod test {

    #[test]
    fn test_pkg_parser() {
        let meadata = super::SmartModuleMetadata::from_file("tests/regex.toml")
            .expect("failed to parse metadata");
    }
}
