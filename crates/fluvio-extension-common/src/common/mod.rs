use clap::Parser;

mod hex_dump;
pub use self::hex_dump::*;

use crate::output::OutputType;

#[derive(Debug, Parser, Default, Clone)]
pub struct OutputFormat {
    /// Output
    #[clap(
        default_value_t,
        short = 'O',
        long = "output",
        value_name = "type",
        value_enum,
        ignore_case = true
    )]
    pub format: OutputType,
}
