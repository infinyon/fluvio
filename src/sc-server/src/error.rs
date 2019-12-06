// error.rs
//  Server Error handling (union of errors used by server)
//

use std::fmt;
use std::io::Error as StdIoError;
use futures::channel::mpsc::SendError;

use kf_socket::KfSocketError;
use k8_config::ConfigError;
use k8_client::ClientError;
use types::PartitionError;
use types::SpuId;

#[derive(Debug)]
pub enum ScServerError {
    IoError(StdIoError),
    SendError(SendError),
    SocketError(KfSocketError),
    K8ConfigError(ConfigError),
    PartitionError(PartitionError),
    K8ClientError(ClientError),
    UnknownSpu(SpuId),
    SpuCommuncationError(SpuId,KfSocketError),    
}

impl fmt::Display for ScServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "{}", err),
            Self::SendError(err) => write!(f,"{}",err),
            Self::SocketError(err) => write!(f,"{}",err),
            Self::K8ConfigError(err) => write!(f,"{}",err),
            Self::PartitionError(err) => write!(f,"{}",err),
            Self::K8ClientError(err) => write!(f,"{}",err),
            Self::UnknownSpu(spu) => write!(f,"unknown spu: {}",spu),
            Self::SpuCommuncationError(id,err) => write!(f,"spu comm error: {}, {}",id,err)   
        }
    }
}

impl From<StdIoError> for ScServerError {
    fn from(error: StdIoError) -> Self {
        ScServerError::IoError(error)
    }
}

impl From<KfSocketError> for ScServerError {
     fn from(error: KfSocketError) -> Self {
        ScServerError::SocketError(error)
    }
}

impl From<SendError> for ScServerError {
    fn from(error: SendError) -> Self {
        ScServerError::SendError(error)
    }
}

impl From<ConfigError> for ScServerError {
    fn from(error: ConfigError) -> Self {
        ScServerError::K8ConfigError(error)
    }
}

impl From<PartitionError> for ScServerError {
    fn from(error: PartitionError) -> Self {
        ScServerError::PartitionError(error)
    }
}

impl From<ClientError> for ScServerError {
    fn from(error: ClientError) -> Self {
        ScServerError::K8ClientError(error)
    }
}
