use std::io::Error as IoError;
use std::fmt;
use serde_json::Error as JsonError;
use fluvio::FluvioError;
use mysql_binlog::errors::BinlogParseError;
use mysql::Error;

#[derive(Debug)]
pub enum CdcError {
    Fluvio(FluvioError),
    IoError(IoError),
    Json(JsonError),
    Binlog(BinlogParseError),
    MySql(Error),
}

impl fmt::Display for CdcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fluvio(err) => write!(f, "{}", err),
            Self::IoError(err) => write!(f, "{}", err),
            Self::Json(err) => write!(f, "{}", err),
            Self::Binlog(err) => write!(f, "{:?}", err),
            Self::MySql(err) => write!(f, "{}", err),
        }
    }
}

impl From<FluvioError> for CdcError {
    fn from(error: FluvioError) -> Self {
        Self::Fluvio(error)
    }
}

impl From<IoError> for CdcError {
    fn from(error: IoError) -> Self {
        Self::IoError(error)
    }
}

impl From<JsonError> for CdcError {
    fn from(error: JsonError) -> Self {
        Self::Json(error)
    }
}

impl From<BinlogParseError> for CdcError {
    fn from(error: BinlogParseError) -> Self {
        Self::Binlog(error)
    }
}

impl From<mysql::Error> for CdcError {
    fn from(error: mysql::Error) -> Self {
        Self::MySql(error)
    }
}
