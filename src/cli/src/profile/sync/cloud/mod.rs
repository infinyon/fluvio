mod login_agent;
use async_std::io;
use structopt::StructOpt;
use futures::AsyncWriteExt;
pub use login_agent::CloudError;

use crate::Terminal;
use crate::CliError;
use crate::{t_print, t_println};
use crate::profile::sync::cloud::login_agent::LoginAgent;

#[derive(Debug, StructOpt)]
pub struct CloudOpt {
    /// Fluvio Cloud email to use for logging in.
    #[structopt(short, long)]
    pub email: Option<String>,
}

pub async fn process_cloud<O>(out: std::sync::Arc<O>, opt: CloudOpt) -> Result<String, CliError>
where
    O: Terminal,
{
    let mut agent = LoginAgent::with_default_path()?;

    // Try downloading a profile using previously-saved credentials
    let result = agent.download_profile().await;
    match result {
        Ok(_profile) => {
            todo!("Download and store profile");
        }
        // If the token is expired, continue to login
        | Err(CloudError::Unauthorized)
        // If we have no token file, continue to login
        | Err(CloudError::UnableToLoadCredentials(_))
        // If we're unable to parse the token file, continue to login
        | Err(CloudError::UnableToParseCredentials(_)) => (),
        Err(other) => return Err(other)?,
    }

    // In case of error, try re-authentication
    let email = match opt.email {
        Some(email) => email,
        None => {
            t_print!(out, "Fluvio Cloud username: ");
            io::stdout().flush().await?;
            let mut email = String::new();
            io::stdin().read_line(&mut email).await?;
            email
        }
    };
    let email = email.trim();
    let password = rpassword::read_password_from_tty(Some("Password: "))?;
    t_println!(out, "Logging in: {:?} - {}", email, password);

    agent.authenticate(email.to_owned(), password).await?;
    if let Ok(_profile) = agent.download_profile().await {
        todo!("Download and store profile");
    }

    Ok("".to_owned())
}
