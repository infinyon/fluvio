pub mod output;
mod common;

pub use common::*;

pub const COMMAND_TEMPLATE: &str = "{about}

{usage}

{all-args}
";

#[macro_export]
macro_rules! t_print {
    ($out:expr,$($arg:tt)*) => ( $out.print(&format!($($arg)*)))
}

#[macro_export]
macro_rules! t_println {
    ($out:expr,$($arg:tt)*) => ( $out.println(&format!($($arg)*)))
}

#[macro_export]
macro_rules! t_print_cli_err {
    ($out:expr,$x:expr) => {
        t_println!($out, "\x1B[1;31merror:\x1B[0m {}", $x);
    };
}
