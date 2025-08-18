pub mod build {
    use anyhow::Result;

    use cargo_builder::package::PackageInfo;
    use cargo_builder::cargo::{Cargo, CargoCommand};

    pub struct BuildOpts {
        pub cloud: bool,
        pub(crate) release: String,
        pub(crate) extra_arguments: Vec<String>,
    }

    impl BuildOpts {
        pub fn with_release(release: &str) -> Self {
            Self {
                cloud: false,
                release: release.to_string(),
                extra_arguments: Vec::default(),
            }
        }
    }

    /// Builds a Connector given it's package info and Cargo Build options
    pub fn build_connector(package_info: &PackageInfo, opts: BuildOpts) -> Result<()> {
        let mut cargo = Cargo::build()
            .profile(opts.release)
            .lib(false)
            .package(package_info.package_name())
            .target(package_info.arch_target())
            .extra_arguments(opts.extra_arguments)
            .build()?;
        if opts.cloud {
            cargo.cmd = CargoCommand::ZigBuild;
        }
        cargo.run()
    }
}

pub mod verify {
    use std::path::Path;
    use std::ffi::OsStr;
    use crate::publish::CONNECTOR_TOML;
    use fluvio_connector_package::metadata::ConnectorMetadata;

    /// check that binary indicated in Connector.toml is in the Package manifest
    pub fn connector_ipkg(ipkg_file: &str) -> Result<(), String> {
        tracing::debug!(pkg = ipkg_file, "reading connector metadata from ipkg file");
        let package_meta = fluvio_hub_util::package_get_meta(ipkg_file)
            .map_err(|err| format!("missing package metadata: {err}"))?;
        let entries: Vec<&Path> = package_meta.manifest.iter().map(Path::new).collect();

        let connector_toml = entries
            .iter()
            .find(|e| e.file_name().eq(&Some(OsStr::new(CONNECTOR_TOML))))
            .ok_or_else(|| format!("Package missing {CONNECTOR_TOML}"))?;

        let connector_toml_bytes =
            fluvio_hub_util::package_get_manifest_file(ipkg_file, connector_toml)
                .map_err(|err| format!("missing connector.toml: {err}"))?;
        let connector_metadata = ConnectorMetadata::from_toml_slice(&connector_toml_bytes)
            .map_err(|err| format!("invalid connector.toml: {err}"))?;
        tracing::trace!("{:#?}", connector_metadata);

        let binary_name = connector_metadata
            .deployment
            .binary
            .as_ref()
            .ok_or_else(|| format!("missing 'binary = \"...\"\' definition in [deployment] section in {CONNECTOR_TOML}"))?;

        let _binary = entries
            .iter()
            .find(|e| e.file_name().eq(&Some(OsStr::new(&binary_name))))
            .ok_or_else(|| format!("Package missing binary file '{binary_name}' specified in {CONNECTOR_TOML} '[deployment]' section"))?;

        Ok(())
    }
}

#[test]
fn test_binary_in_manifest() {
    let ipkg_test_set = vec![
        // (ipkg file, Option<errstr>)

        // this package was built with binary "foo", but the Connector.toml specifies "no-foo"
        (
            "../../crates/cdk/tests/foo-0.1.0.ipkg",
            Some(
                "Package missing binary file 'no-foo' specified in Connector.toml '[deployment]' section",
            ),
        ),
    ];

    for (ipkg_file, opt_err_str) in ipkg_test_set {
        let result = verify::connector_ipkg(ipkg_file);
        println!("{result:?}");
        if let Some(err_str) = opt_err_str {
            assert!(result.is_err());
            assert_eq!(result.unwrap_err(), err_str);
        } else {
            assert!(result.is_ok());
        }
    }
}
