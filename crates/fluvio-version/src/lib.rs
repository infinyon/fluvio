/// The `git rev-parse HEAD` of the current build of Fluvio.
pub const GIT_HASH: &str = env!("GIT_HASH");

/// The current version of Fluvio.
pub const VERSION: &str = include_str!("../../../VERSION");

pub mod build {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
