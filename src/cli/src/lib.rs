
#![type_length_limit="1101663"]

mod common;
mod error;
mod consume;
mod produce;
mod root_cli;
mod spu;
mod topic;
mod advanced;
mod output;
mod profile;


pub use self::error::CliError;
pub use self::root_cli::run_cli;

use output::Terminal;
use output::*;

#[macro_export]
macro_rules! t_println {
    ($out:expr,$($arg:tt)*) => ( $out.println(&format!($($arg)*)))
}

#[macro_export]
macro_rules! t_print_cli_err {
    ($out:expr,$x:expr) => {
        t_println!($out,"\x1B[1;31merror:\x1B[0m {}", $x);
    };
}