//!
//! # Config helper
//!
use std::path::PathBuf;

use types::defaults::CONFIG_FILE_EXTENTION;
use types::defaults::{SERVER_CONFIG_BASE_PATH, SERVER_CONFIG_DIR};

/// generate server configuration file
pub fn build_server_config_file_path(file_name: &'static str) -> PathBuf {
    let mut config_file_path = PathBuf::new();

    // stitch-up default configuration file path
    config_file_path.push(SERVER_CONFIG_BASE_PATH);
    config_file_path.push(SERVER_CONFIG_DIR);
    config_file_path.push(file_name);
    config_file_path.set_extension(CONFIG_FILE_EXTENTION);

    config_file_path
}

//
// Unit Tests
//

#[cfg(test)]
pub mod test {
    use super::*;
    use types::defaults::{SC_CONFIG_FILE, SPU_CONFIG_FILE};

    #[test]
    fn test_build_sc_server_config_file_path() {
        let sc_server_file_path = build_server_config_file_path(SC_CONFIG_FILE);

        let mut expected_file_path = PathBuf::new();
        expected_file_path.push("/etc/fluvio/sc_server.toml");

        assert_eq!(sc_server_file_path, expected_file_path);
    }

    #[test]
    fn test_build_spu_server_config_file_path() {
        let spu_server_file_path = build_server_config_file_path(SPU_CONFIG_FILE);

        let mut expected_file_path = PathBuf::new();
        expected_file_path.push("/etc/fluvio/spu_server.toml");

        assert_eq!(spu_server_file_path, expected_file_path);
    }
}
