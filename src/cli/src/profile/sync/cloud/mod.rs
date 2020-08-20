use std::io;
use std::io::Write;
mod login_agent;
use structopt::StructOpt;
pub use login_agent::CloudError;

use tracing::info;

use crate::Terminal;
use crate::CliError;
use crate::t_print;
use crate::profile::sync::cloud::login_agent::LoginAgent;
use fluvio::config::{Cluster, ConfigFile, Profile};
use crate::profile::{process_switch, SwitchOpt};

#[derive(Debug, StructOpt)]
pub struct CloudOpt {
    /// Fluvio Cloud email to use for logging in.
    #[structopt(short, long)]
    pub email: Option<String>,
    /// Fluvio Cloud remote address to use.
    #[structopt(long)]
    pub remote: Option<String>,
}

pub async fn process_cloud<O>(out: std::sync::Arc<O>, opt: CloudOpt) -> Result<String, CliError>
where
    O: Terminal,
{
    let mut agent = match opt.remote {
        Some(remote) => LoginAgent::with_default_path()?.with_remote(remote),
        None => LoginAgent::with_default_path()?,
    };

    // Try downloading a profile using previously-saved credentials
    let result = agent.download_profile().await;
    match result {
        Ok(cluster) => {
            let result = save_cluster(out.clone(), cluster)?;
            return Ok(result);
        }
        // If the token is expired, continue to login
        | Err(CloudError::Unauthorized)
        // If we have no token file, continue to login
        | Err(CloudError::UnableToLoadCredentials(_))
        // If we're unable to parse the token file, continue to login
        | Err(CloudError::UnableToParseCredentials(_)) => (),
        Err(other) => return Err(other.into()),
    }

    // In case of error, try re-authentication
    let email = match opt.email {
        Some(email) => email,
        None => {
            t_print!(out, "Fluvio Cloud email: ");
            io::stdout().flush()?;
            let mut email = String::new();
            io::stdin().read_line(&mut email)?;
            email
        }
    };
    let email = email.trim();
    let password = rpassword::read_password_from_tty(Some("Password: "))?;

    agent.authenticate(email.to_owned(), password).await?;
    if let Ok(cluster) = agent.download_profile().await {
        let result = save_cluster(out, cluster)?;
        return Ok(result);
    }

    Ok("".to_string())
}

fn save_cluster<O: Terminal>(out: std::sync::Arc<O>, cluster: Cluster) -> Result<String, CliError> {
    let mut config_file = ConfigFile::load_default_or_new()?;
    let profile = Profile::new("fluvio-cloud".to_string());
    config_file
        .mut_config()
        .add_cluster(cluster, "fluvio-cloud".to_string());
    config_file
        .mut_config()
        .add_profile(profile, "fluvio-cloud".to_string());
    config_file.save()?;
    info!("Successfully saved fluvio-cloud profile");
    process_switch(
        out,
        SwitchOpt {
            profile_name: "fluvio-cloud".to_string(),
        },
    )?;
    Ok("Successfully saved fluvio-cloud profile".to_string())
}
