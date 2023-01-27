// error.rs
//  Server Error handling (union of errors used by server)
//

use std::fmt;
use std::io::Error as IoError;

use fluvio_types::PartitionError;
use k8_client::ClientError;
use fluvio_socket::SocketError;
use fluvio_auth::AuthError;

#[derive(Debug)]
pub enum ScError {
    Io(IoError),
    Client(ClientError),
    Socket(SocketError),
    Partition(PartitionError),
    Auth(AuthError),
}

impl fmt::Display for ScError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            //    Self::SendError(err) => write!(f, "{}", err),
            Self::Client(err) => write!(f, "{err}"),
            Self::Socket(err) => write!(f, "{err}"),
            Self::Partition(err) => write!(f, "{err}"),
            Self::Auth(err) => write!(f, "{err}"),
        }
    }
}

impl From<IoError> for ScError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl From<AuthError> for ScError {
    fn from(error: AuthError) -> Self {
        Self::Auth(error)
    }
}

impl From<ClientError> for ScError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl From<SocketError> for ScError {
    fn from(error: SocketError) -> Self {
        Self::Socket(error)
    }
}

impl From<PartitionError> for ScError {
    fn from(error: PartitionError) -> Self {
        Self::Partition(error)
    }
}
