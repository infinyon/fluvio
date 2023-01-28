pub mod setup;
pub mod test_runner;
pub mod tls;

pub mod test_meta;
use once_cell::sync::Lazy;

static VERSION: Lazy<String> = Lazy::new(|| {
    let version = include_str!("../../VERSION");
    match option_env!("FLUVIO_VERSION_SUFFIX") {
        Some(suffix) => format!("{version}-{suffix}"),
        None => version.to_string(),
    }
});
