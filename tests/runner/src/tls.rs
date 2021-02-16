use std::path::PathBuf;
use fluvio::config::{TlsPolicy, TlsPaths};

pub fn cert_dir() -> PathBuf {
    std::env::current_dir().unwrap().join("tls").join("certs")
}

pub fn load_tls(client_user: &str) -> (TlsPolicy, TlsPolicy) {
    const DOMAIN: &str = "fluvio.local";
    let cert_dir = cert_dir();
    let client_policy = TlsPolicy::from(TlsPaths {
        domain: DOMAIN.to_string(),
        key: cert_dir.join(format!("client-{}.key", client_user)),
        cert: cert_dir.join(format!("client-{}.crt", client_user)),
        ca_cert: cert_dir.join("ca.crt"),
    });
    let server_policy = TlsPolicy::from(TlsPaths {
        domain: DOMAIN.to_string(),
        key: cert_dir.join("server.key"),
        cert: cert_dir.join("server.crt"),
        ca_cert: cert_dir.join("ca.crt"),
    });
    (client_policy, server_policy)
}

pub struct Cert {
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl Cert {
    pub fn load_client(client_user: &str) -> Self {
        let cert_dir = cert_dir();
        Cert {
            ca: cert_dir.join("ca.crt"),
            cert: cert_dir.join(format!("client-{}.crt", client_user)),
            key: cert_dir.join(format!("client-{}.key", client_user)),
        }
    }

    pub fn load_server() -> Self {
        let cert_dir = cert_dir();
        Cert {
            ca: cert_dir.join("ca.crt"),
            cert: cert_dir.join("server.crt"),
            key: cert_dir.join("server.key"),
        }
    }
}
