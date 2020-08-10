use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct LoginOpt {
    /// Fluvio Cloud username to use for logging in.
    #[structopt(short, long = "user")]
    pub username: String,
}

pub struct LogoutOpt;
