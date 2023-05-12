pub mod build {
    use anyhow::Result;

    use cargo_builder::package::PackageInfo;
    use cargo_builder::cargo::Cargo;

    pub struct BuildOpts {
        pub(crate) release: String,
        pub(crate) extra_arguments: Vec<String>,
    }

    impl BuildOpts {
        pub fn with_release(release: &str) -> Self {
            Self {
                release: release.to_string(),
                extra_arguments: Vec::default(),
            }
        }
    }

    /// Builds a Connector given it's package info and Cargo Build options
    pub fn build_connector(package_info: &PackageInfo, opts: BuildOpts) -> Result<()> {
        let cargo = Cargo::build()
            .profile(opts.release)
            .lib(false)
            .package(package_info.package_name())
            .target(package_info.arch_target())
            .extra_arguments(opts.extra_arguments)
            .build()?;

        cargo.run()
    }
}
