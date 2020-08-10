mod cli;

pub use cli::LoginOpt;
pub use cli::LogoutOpt;
pub use process::process_login;
pub use process::process_logout;

mod process {
    use crate::CliError;
    use crate::Terminal;
    use crate::cloud::{LoginOpt, LogoutOpt};
    use crate::t_println;

    pub async fn process_login<O>(out: std::sync::Arc<O>, opt: LoginOpt) -> Result<String, CliError>
    where
        O: Terminal,
    {
        let password = rpassword::read_password_from_tty(Some("Fluvio Password: "))?;
        t_println!(out, "Logging in: {} - {}", opt.username, password);
        Ok("".to_owned())
    }

    pub async fn process_logout<O>(
        out: std::sync::Arc<O>,
        _opt: LogoutOpt,
    ) -> Result<String, CliError>
    where
        O: Terminal,
    {
        t_println!(out, "Logging out");
        Ok("".to_owned())
    }
}
