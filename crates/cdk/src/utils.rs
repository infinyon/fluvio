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
