mod cli;

pub use cli::LoginOpt;
pub use process::process_login;

mod process {
    use crate::CliError;
    use crate::Terminal;
    use crate::login::LoginOpt;
    use crate::t_println;

    pub async fn process_login<O>(
        out: std::sync::Arc<O>,
        opt: LoginOpt,
    ) -> Result<String, CliError>
        where O: Terminal,
    {
        let password = rpassword::read_password_from_tty(Some("Fluvio Password: "))?;

        t_println!(out, "Logging in: {} - {}", opt.username, password);

        Ok("".to_owned())
    }
}