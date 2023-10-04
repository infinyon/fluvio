pub mod setup;
pub mod test_runner;
pub mod tls;

pub mod test_meta;
use once_cell::sync::Lazy;
use semver::Version;

static FLUVIO_PLATFORM_TEST_VERSION: Lazy<Version> = Lazy::new(|| {
    let version = env!("FLUVIO_PLATFORM_TEST_VERSION").trim();
    Version::parse(version)
        .expect("Env variable 'FLUVIO_PLATFORM_TEST_VERSION' should contain a valid semver.")
});
