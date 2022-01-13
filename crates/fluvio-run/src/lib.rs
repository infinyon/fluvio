use structopt::StructOpt;

mod error;

pub use error::RunnerError;
use error::Result;
use fluvio_spu::SpuOpt;
use fluvio_sc::cli::ScOpt;
use fluvio_extension_common::FluvioExtensionMetadata;

const VERSION: &str = include_str!("../../../VERSION");

#[derive(Debug, StructOpt)]
#[structopt(version = crate::VERSION)]
pub enum RunCmd {
    /// Run a new Streaming Processing Unit (SPU)
    #[structopt(name = "spu")]
    SPU(SpuOpt),
    /// Run a new Streaming Controller (SC)
    #[structopt(name = "sc")]
    SC(ScOpt),
    /// Return plugin metadata as JSON
    #[structopt(name = "metadata")]
    Metadata(MetadataOpt),

    /// Print version information
    #[structopt(name = "version")]
    Version(VersionOpt),
}

impl RunCmd {
    pub fn process(self) -> Result<()> {
        match self {
            Self::SPU(opt) => {
                fluvio_spu::main_loop(opt);
            }
            Self::SC(opt) => {
                fluvio_sc::k8::main_k8_loop(opt);
            }
            Self::Metadata(meta) => {
                meta.process()?;
            },
            Self::Version(opt) => {
                opt.process()?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub struct MetadataOpt {}
impl MetadataOpt {
    pub fn process(self) -> Result<()> {
        if let Ok(metadata) = serde_json::to_string(&Self::metadata()) {
            println!("{}", metadata);
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

#[derive(Debug, StructOpt)]
pub struct VersionOpt {}

impl VersionOpt {
    pub fn process(self) -> Result<()> {
       

        Ok(())
    }
}