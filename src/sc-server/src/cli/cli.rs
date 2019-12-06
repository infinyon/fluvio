//!
//! # CLI for Streaming Controller
//!
//! Command line interface to provision SC id and bind-to server/port.
//! Parameters are overwritten in the following sequence:
//!     1) default values
//!     2) custom configuration if provided, or default configuration (if not)
//!     3) cli parameters
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::process;

use log::info;
use structopt::StructOpt;

use types::print_cli_err;
use types::socket_helpers::string_to_socket_addr;
use k8_config::K8Config;

use crate::ScServerError;
use super::ScConfig;

/// cli options
#[derive(Debug, StructOpt)]
#[structopt(name = "sc-server",  about = "Streaming Controller")]
pub struct ScOpt {
    #[structopt(short = "i", long = "id", value_name = "integer")]
    /// Unique identifier of the server
    pub id: Option<i32>,

    #[structopt(short = "b", long = "bind-public", value_name = "host:port")]
    /// Address for external communication
    pub bind_public: Option<String>,

    #[structopt(short = "f", long = "conf", value_name = "file")]
    /// Configuration file
    pub config_file: Option<String>,

    #[structopt(short = "n", long = "namespace", value_name = "namespace")]
    pub namespace: Option<String>
}

/// validate streaming controller cli inputs and generate ScConfig
pub fn get_sc_config() -> Result<(ScConfig,K8Config), ScServerError> {
    sc_opt_to_sc_config(ScOpt::from_args())
}


/// convert cli options to sc_config
fn sc_opt_to_sc_config(opt: ScOpt) -> Result<(ScConfig,K8Config), ScServerError> {

    let mut sc_config = ScConfig::new(opt.config_file)?;

    let k8_config = K8Config::load().expect("no k8 config founded");

    sc_config.namespace = k8_config.namespace().to_owned();
    info!("using {} as namespace from kubernetes config",sc_config.namespace);


    // override id if set
    if let Some(id) = opt.id {
        if id < 0 {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Id must greater of equal to 0",
            ).into());
        }
        sc_config.id = id;
    }

    // override public if set
    if let Some(bind_public) = opt.bind_public {

        let addr = string_to_socket_addr(&bind_public).map_err(
            |err| IoError::new(
                ErrorKind::InvalidInput,
                format!("problem resolving public bind {}'", err),
            ))?;
       
        sc_config.public_endpoint = addr.into();
    }


    // override namespace if set
    if let Some(namespace) = opt.namespace {
        sc_config.namespace = namespace;
    }

    info!("sc config: {:#?}",sc_config);

    Ok((sc_config,k8_config))
}

/// return SC configuration or exist program.
pub fn parse_cli_or_exit() -> (ScConfig,K8Config) {
    match get_sc_config() {
        Err(err) => {
            print_cli_err!(err);
            process::exit(0x0100);
        }
        Ok(config) => config,
    }
}

// ---------------------------------------
// Unit Tests
// ---------------------------------------

#[cfg(test)]
pub mod test {
    
    use std::net::{IpAddr, Ipv4Addr};
    use std::net::SocketAddr;
    
    use types::socket_helpers::EndPoint;

    use super::ScOpt;
    use super::sc_opt_to_sc_config;
    use super::ScConfig;

    #[test]
    fn test_get_sc_config_no_params() {
        let sc_opt = ScOpt {
            id: None,
            bind_public: None,
            config_file: None,
            namespace: Some("test".to_owned())
        };

        // test read & parse
        let result = sc_opt_to_sc_config(sc_opt);
        assert!(result.is_ok());

        // compare with expected result
        let expected = ScConfig {
            id: 1,
            public_endpoint:  EndPoint::all_end_point(9003),
            namespace: "test".to_owned(),
            ..Default::default()
        };

        assert_eq!(result.unwrap().0, expected);
    }

    #[test]
    fn test_get_sc_config_from_config_file() {
        let sc_opt = ScOpt {
            id: None,
            bind_public: None,
            config_file: Some("./test-data/config/sc_server.toml".to_owned()),
            namespace: Some("test".to_owned())
        };

        // test read & parse
        let result = sc_opt_to_sc_config(sc_opt);
        assert!(result.is_ok());

        // compare with expected result
        let expected = ScConfig {
            id: 500,
            public_endpoint: EndPoint::local_end_point(9999),
            namespace: "test".to_owned(),
             ..Default::default()
        };

        assert_eq!(result.unwrap().0, expected);
    }

    #[test]
    fn test_get_sc_config_overwite_config_file() {
        let sc_opt = ScOpt {
            id: Some(100),
            bind_public: Some("1.1.1.1:8888".to_owned()),
            config_file: Some("./test-data/config/sc_server.toml".to_owned()),
            namespace: Some("test".to_owned())
        };

        // test read & parse
        let result = sc_opt_to_sc_config(sc_opt);
        assert!(result.is_ok());

        // compare with expected result
        let expected = ScConfig {
            id: 100,
            public_endpoint: (SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 8888)).into(),
            namespace: "test".to_owned(),
            ..Default::default()
        };

        assert_eq!(result.unwrap().0, expected);
    }

}
//        println!("{:#?}", result);
