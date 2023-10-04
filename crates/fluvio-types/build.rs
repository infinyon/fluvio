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
    println!("cargo:rustc-env=FLUVIO_PLATFORM_VERSION={version}");
}
