use structopt::StructOpt;
use fluvio_spu::SpuOpt;
use fluvio_sc::cli::ScOpt;

use crate::Result;

#[derive(Debug, StructOpt)]
pub enum RunOpt {
    #[structopt(about = "Run SPU server")]
    SPU(SpuOpt),
    #[structopt(about = "Run streaming controller")]
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
