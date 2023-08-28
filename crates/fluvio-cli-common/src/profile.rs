use fluvio::config::ConfigFile;

/// Retrieves Fluvio Profile name from the ConfigFile
pub fn profile_name(config_file: &ConfigFile) -> Option<String> {
    config_file
        .config()
        .current_profile_name()
        .map(|name| name.to_string())
}
