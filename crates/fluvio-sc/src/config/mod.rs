mod sc_config;

pub use self::sc_config::ScConfig;
pub use self::sc_config::ScConfigBuilder;
pub use self::sc_config::DEFAULT_NAMESPACE;

macro_rules! whitelist {
    ($config:expr,$name:expr,$start:expr) => {
        if $config.enabled($name) {
            $start;
        }
    };
}
