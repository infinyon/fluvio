mod hex_dump;

pub use self::hex_dump::*;

pub use output::*;

mod output {

    use structopt::StructOpt;

    use crate::OutputType;


#[derive(Debug, StructOpt, Default)]
    pub struct OutputFormat {

        /// Output
        #[structopt(
            short = "O",
            long = "output",
            value_name = "type",
            possible_values = &OutputType::variants(),
            case_insensitive = true
        )]
        output: Option<OutputType>,
    }

    impl OutputFormat {
        pub fn as_output(self) -> OutputType {
            self.output.unwrap_or(OutputType::default())
        }
    }
}