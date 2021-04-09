use structopt::StructOpt;

mod error;

pub use error::RunnerError;
use error::Result;
use fluvio_spu::SpuOpt;
use fluvio_sc::cli::ScOpt;

#[derive(Debug, StructOpt)]
pub enum RunCmd {
    /// Run a new Streaming Processing Unit (SPU)
    #[structopt(name = "spu")]
    SPU(SpuOpt),
    /// Run a new Streaming Controller (SC)
    #[structopt(name = "sc")]
    SC(ScOpt),
}

impl RunCmd {
    pub async fn process(self) -> Result<()> {
        match self {
            Self::SPU(opt) => {
                fluvio_spu::main_loop(opt);
            }
            Self::SC(opt) => {
                fluvio_sc::k8::main_k8_loop(opt);
            }
        }
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub struct SpuLivenessCheckCmd {
    #[structopt()]
    endpoint: std::net::SocketAddr,
}

impl SpuLivenessCheckCmd {
    pub async fn process(self) -> Result<()> {
        fluvio_future::subscriber::init_tracer(None);
        fluvio_spu::probe(self.endpoint)?;
        Ok(())
    }
}
