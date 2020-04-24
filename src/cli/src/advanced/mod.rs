mod generate;
mod run;
mod request_api;

use structopt::StructOpt;

use generate::GenerateTemplateOpt;
use run::RunRequestOpt;
use request_api::RequestApi;

pub use generate::process_generate_template;
pub use run::process_run_request;
use request_api::parse_and_pretty_from_file;

use crate::Terminal;
use crate::error::CliError;

#[derive(Debug, StructOpt)]
pub enum AdvancedOpt {
    #[structopt(name = "generate", template = "{about}

{usage}

{all-args}
", about = "Generate a request template")]
    Generate(GenerateTemplateOpt),

    #[structopt(name = "run",  template = "{about}

{usage}

{all-args}
", about = "Send request to server")]
    Run(RunRequestOpt),
}

pub async fn process_advanced<O>(out: std::sync::Arc<O>,opt: AdvancedOpt) -> Result<String, CliError>
    where O: Terminal
{
    (match opt {
        AdvancedOpt::Generate(generate_opt) => process_generate_template(out,generate_opt),
        AdvancedOpt::Run(run_opt) => process_run_request(out,run_opt).await
    }).map(|_| format!(""))
}
