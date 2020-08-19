use structopt::StructOpt;
use fluvio_spu::SpuOpt;
use fluvio_sc::cli::ScOpt;

use crate::CliError;

#[derive(Debug, StructOpt)]
pub enum RunOpt {
    #[structopt(about = "Run SPU server")]
    SPU(SpuOpt),
    #[structopt(about = "Run streaming controller")]
    SC(ScOpt),
}

pub fn process_run(run_opt: RunOpt) -> Result<String, CliError> {
    match run_opt {
        RunOpt::SPU(opt) => fluvio_spu::main_loop(opt),
        RunOpt::SC(opt) => fluvio_sc::k8::main_k8_loop(opt),
    }

    Ok("".to_owned())
}
