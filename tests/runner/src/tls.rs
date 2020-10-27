use std::path::PathBuf;
use std::process::Command;

use crate::TestOption;

#[derive(Clone)]
pub struct TlsLoader {
    option: TestOption,
}

impl TlsLoader {
    pub fn new(option: TestOption) -> Self {
        Self { option }
    }

    pub fn set_client_tls(&self, cmd: &mut Command) {
        let client_dir = Cert::load_client(&self.option.tls_user);

        cmd.arg("--tls")
            .arg("--domain")
            .arg("fluvio.local")
            .arg("--enable-client-cert")
            .arg("--ca-cert")
            .arg(client_dir.ca.as_os_str())
            .arg("--client-cert")
            .arg(client_dir.cert.as_os_str())
            .arg("--client-key")
            .arg(client_dir.key.as_os_str());
    }

    pub fn setup_server_tls(&self, cmd: &mut Command) {
        if self.option.tls() {
            let server_dir = Cert::load_server();
            cmd.arg("--tls")
                .arg("--enable-client-cert")
                .arg("--ca-cert")
                .arg(server_dir.ca.as_os_str())
                .arg("--server-cert")
                .arg(server_dir.cert.as_os_str())
                .arg("--server-key")
                .arg(server_dir.key.as_os_str());
        }
    }
}

pub fn cert_dir() -> PathBuf {
    std::env::current_dir().unwrap().join("tls").join("certs")
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
