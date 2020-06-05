mod hex_dump;

pub use self::hex_dump::bytes_to_hex_dump;
pub use self::hex_dump::hex_dump_separator;

pub use cli::*;

mod cli {

    use structopt::StructOpt;

    #[derive(Debug, StructOpt)]
    pub struct KfConfig {
        #[cfg(feature = "kf")]
        #[structopt(
            short = "k",
            long = "kf",
            value_name = "host:port",
            conflicts_with = "sc"
        )]
        pub kf: Option<String>,
    }
}
