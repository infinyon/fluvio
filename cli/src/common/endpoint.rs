//!
//! # CLI Endpoint
//!
//! Endpoint data structure
//!
use std::fmt;

use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    pub fn new(host: &String, port: &u16) -> Self {
        Endpoint {
            host: host.clone(),
            port: *port,
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}
