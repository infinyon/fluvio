use structopt::StructOpt;

mod hex_dump;
pub use self::hex_dump::*;
use crate::OutputType;

#[derive(Debug, StructOpt, Default)]
pub struct OutputFormat {
    /// Output
    #[structopt(
        default_value,
        short = "O",
        long = "output",
        value_name = "type",
        possible_values = &OutputType::variants(),
        case_insensitive = true
    )]
    pub format: OutputType,
}
