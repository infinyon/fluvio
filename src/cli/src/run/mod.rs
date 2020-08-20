use structopt::StructOpt;
use fluvio_dataplane::SpuOpt;
use fluvio_controlplane_cli::ScOpt;

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
        RunOpt::SPU(opt) => fluvio_dataplane::main_loop(opt),
        RunOpt::SC(opt) => fluvio_controlplane_cli::main_loop(opt),
    }

    Ok("".to_owned())
}
