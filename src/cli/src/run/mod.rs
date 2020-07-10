use structopt::StructOpt;
use flv_spu::SpuOpt;
use flv_sc_k8::ScOpt;

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
        RunOpt::SPU(opt) => flv_spu::main_loop(opt),
        RunOpt::SC(opt) => flv_sc_k8::main_loop(opt),
    }

    Ok("".to_owned())
}
