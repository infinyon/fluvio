use semver::Version;

fn main() {
    if let Ok(verpath) = std::fs::canonicalize("../../VERSION") {
        if verpath.exists() {
            println!("cargo:rerun-if-changed=../../VERSION");
        }
    }

    println!("cargo:rerun-if-changed=build.rs");

    // Parse the Fluvio Platform Version
    let version = include_str!("../../VERSION").trim();
    let version = Version::parse(version)
        .unwrap_or_else(|_| panic!("Fluvio Platform VERSION '{version}' is not a valid semver"));

    // Append an optional version suffix
    let test_fluvio_platform_version =
        match option_env!("FLUVIO_VERSION_SUFFIX").filter(|s| !s.is_empty()) {
            Some(suffix) => Version::parse(&format!("{version}-{suffix}"))
                .expect("Invalid value in 'FLUVIO_VERSION_SUFFIX' ENV variable."),
            None => version,
        };
    println!("cargo:rustc-env=FLUVIO_PLATFORM_TEST_VERSION={test_fluvio_platform_version}");
}
