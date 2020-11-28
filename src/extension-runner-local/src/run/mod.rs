use structopt::StructOpt;
use fluvio_spu::SpuOpt;
use fluvio_sc::cli::ScOpt;

use crate::Result;

#[derive(Debug, StructOpt)]
pub enum RunnerCmd {
    /// Run a Streaming Controller (SC) or SPU
    #[structopt(name = "run")]
    Run(RunOpt),
}

impl RunnerCmd {
    pub async fn process(self) -> Result<()> {
        match self {
            Self::Run(opt) => {
                opt.process().await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub enum RunOpt {
    /// Run a new Streaming Controller (SC)
    #[structopt(name = "spu")]
    SPU(SpuOpt),
    /// Run a new Streaming Processing Unit (SPU)
    #[structopt(name = "sc")]
    SC(ScOpt),
}

impl RunOpt {
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
