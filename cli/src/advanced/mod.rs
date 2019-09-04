mod generate;
mod run;
mod request_api;

use structopt::StructOpt;

use generate::GenerateTemplateOpt;
use run::RunRequestOpt;
use request_api::RequestApi;

pub use generate::process_generate_template;
pub use run::process_run_request;
pub use request_api::send_request_to_server;
pub use request_api::parse_request_from_file;

use crate::error::CliError;

#[derive(Debug, StructOpt)]
pub enum AdvancedOpt {
    #[structopt(name = "generate", author = "", template = "{about}

{usage}

{all-args}
", about = "Generate a request template")]
    Generate(GenerateTemplateOpt),

    #[structopt(name = "run", author = "", template = "{about}

{usage}

{all-args}
", about = "Send request to server")]
    Run(RunRequestOpt),
}

pub fn process_advanced(opt: AdvancedOpt) -> Result<(), CliError> {
    match opt {
        AdvancedOpt::Generate(generate_opt) => process_generate_template(generate_opt),
        AdvancedOpt::Run(run_opt) => process_run_request(run_opt),
    }
}
