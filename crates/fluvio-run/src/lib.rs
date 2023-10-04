use clap::Parser;

mod error;

pub use error::RunnerError;
use error::Result;
use fluvio_spu::SpuOpt;
use fluvio_sc::cli::ScOpt;
use fluvio_extension_common::FluvioExtensionMetadata;
use fluvio_types::FLUVIO_PLATFORM_VERSION;

#[derive(Debug, Parser)]
#[command(version = FLUVIO_PLATFORM_VERSION.to_string())]
pub enum RunCmd {
    /// Run a new Streaming Processing Unit (SPU)
    #[command(name = "spu")]
    SPU(SpuOpt),
    /// Run a new Streaming Controller (SC)
    #[command(name = "sc")]
    SC(ScOpt),
    /// Return plugin metadata as JSON
    #[command(name = "metadata")]
    Metadata(MetadataOpt),

    /// Print version information
    #[command(name = "version")]
    Version(VersionOpt),
}

impl RunCmd {
    pub fn process(self) -> Result<()> {
        match self {
            Self::SPU(opt) => {
                fluvio_spu::main_loop(opt);
            }
            Self::SC(opt) => {
                fluvio_sc::start::main_loop(opt);
            }
            Self::Metadata(meta) => {
                meta.process()?;
            }
            Self::Version(opt) => {
                opt.process()?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Parser)]
pub struct MetadataOpt {}
impl MetadataOpt {
    pub fn process(self) -> Result<()> {
        if let Ok(metadata) = serde_json::to_string(&Self::metadata()) {
            println!("{metadata}");
        }
        Ok(())
    }

    pub fn metadata() -> FluvioExtensionMetadata {
        FluvioExtensionMetadata {
            title: "Fluvio Runner".into(),
            package: Some("fluvio/fluvio-run".parse().unwrap()),
            description: "Run Fluvio cluster components (SC and SPU)".into(),
            version: semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
        }
    }
}

#[derive(Debug, Parser)]
pub struct VersionOpt {}

impl VersionOpt {
    pub fn process(self) -> Result<()> {
        println!("Git Commit: {}", env!("GIT_HASH"));
        println!("Platform Version: {}", *FLUVIO_PLATFORM_VERSION);

        Ok(())
    }
}
