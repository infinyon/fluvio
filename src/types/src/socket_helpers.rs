use std::fmt;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::net::Ipv4Addr;
use std::net::IpAddr;

use log::debug;
use log::error;

//
// Structures
//
#[derive(Debug, PartialEq, Clone)]
pub struct ServerAddress {
    pub host: String,
    pub port: u16,
}

impl ServerAddress {

    pub fn new<S>(host: S,port: u16) -> Self where S: Into<String>{
        Self {
            host: host.into(),
            port
        }
    }
}

impl fmt::Display for ServerAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl TryFrom<String> for ServerAddress {
    type Error = IoError;

    fn try_from(host_port: String) -> Result<Self, Self::Error> {
        let v: Vec<&str> = host_port.split(':').collect();

        if v.len() != 2 {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("invalid host:port format {}", host_port).as_str(),
            ));
        }

        Ok(ServerAddress {
            host: v[0].to_string(),
            port: v[1]
                .parse::<u16>()
                .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))?,
        })
    }
}

impl TryFrom<ServerAddress> for SocketAddr {
    type Error = IoError;

    fn try_from(endpoint: ServerAddress) -> Result<Self, Self::Error> {
        host_port_to_socket_addr(&endpoint.host, endpoint.port)
    }
}

// converts a host/port to SocketAddress
pub fn server_to_socket_addr(server_addr: &ServerAddress) -> Result<SocketAddr, IoError> {
    host_port_to_socket_addr(&server_addr.host, server_addr.port)
}

// converts a host/port to SocketAddress
pub fn host_port_to_socket_addr(host: &str, port: u16) -> Result<SocketAddr, IoError> {
    let addr_string = format!("{}:{}", host, port);
    string_to_socket_addr(&addr_string)
}

/// convert string to socket addr
pub fn string_to_socket_addr(addr_string: &str) -> Result<SocketAddr, IoError> {
    debug!("resolving host: {}",addr_string);
    match addr_string.to_socket_addrs() {
        Err(err) => {
            error!("error resolving addr: {} {}",addr_string,err);
            Err(err)
        },
        Ok(mut addrs_iter) => {
            match addrs_iter.next() {
                Some(addr) => {
                    debug!("resolved: {}",addr);
                    Ok(addr)
                },
                None => {
                    error!("error resolving addr: {}",addr_string);
                    Err(IoError::new(
                        ErrorKind::InvalidInput,
                        format!("host/port cannot be resolved {}", addr_string).as_str(),
                    ))
                }
            }
        }
    
    }


}

#[derive(Debug, PartialEq, Clone)]
pub enum EndPointEncryption {
    PLAINTEXT,
}

impl Default for EndPointEncryption {
    fn default() -> Self {
        EndPointEncryption::PLAINTEXT
    }
}

impl fmt::Display for EndPointEncryption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Plain")
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct EndPoint {
    pub addr: SocketAddr,
    pub encryption: EndPointEncryption,
}

impl EndPoint {
    /// Build endpoint for local server
    pub fn local_end_point(port: u16) -> Self {
        Self {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
            encryption: EndPointEncryption::default(),
        }
    }

    /// listen on 0.0.0.0
    pub fn all_end_point(port: u16) -> Self {
        Self {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port),
            encryption: EndPointEncryption::default(),
        }
    }
}

impl From<SocketAddr> for EndPoint {
    fn from(addr: SocketAddr) -> Self {
        EndPoint {
            addr,
            encryption: EndPointEncryption::default(),
        }
    }
}

impl TryFrom<&str> for EndPoint {
    type Error = IoError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        string_to_socket_addr(value).map(|addr| EndPoint {
            addr,
            encryption: EndPointEncryption::PLAINTEXT,
        })
    }
}

impl fmt::Display for EndPoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", self.addr, self.encryption)
    }
}
