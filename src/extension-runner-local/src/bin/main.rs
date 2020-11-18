use structopt::StructOpt;
use fluvio_future::task::run_block_on;
use fluvio_runner_local::{RunCmd, RunnerError};

fn main() {
    fluvio_future::subscriber::init_tracer(None);

    let cmd = RunnerCmd::from_args();
    run_block_on(async {
        cmd.process().await.expect("process should run");
    });
}

#[derive(Debug, StructOpt)]
pub enum RunnerCmd {
    /// Run a Streaming Controller (SC) or SPU
    #[structopt(name = "run")]
    Run(RunCmd),
}

impl RunnerCmd {
    pub async fn process(self) -> Result<(), RunnerError> {
        match self {
            Self::Run(opt) => {
                opt.process().await?;
            }
        }
        Ok(())
    }
}
