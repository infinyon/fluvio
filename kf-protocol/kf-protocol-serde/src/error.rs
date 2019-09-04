use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt;
use std::error::Error as StdError;
use serde::de::Error as DeError;
use std::io::Error as IoError;

pub type Error = Box<ErrorKind>;

#[derive(Debug)]
pub enum ErrorKind {
    Io(IoError),
    DeserializeAnyNotSupported,
    InvalidBoolEncoding(u8),
    NotEnoughBytes,
    NotSupportedFormat(String),
    Custom(String)
}


impl StdError for ErrorKind {
    fn description(&self) -> &str {
        match *self {
            ErrorKind::Io(ref err) => StdError::description(err),
            ErrorKind::NotEnoughBytes => "not enough bytes",
            ErrorKind::DeserializeAnyNotSupported => {
                "Kafka doesn't support serde::Deserializer::deserialize_any"
            },
            ErrorKind::InvalidBoolEncoding(_) => "invalid u8 while decoding bool",
            ErrorKind::Custom(ref msg) => msg ,
            ErrorKind::NotSupportedFormat(ref msg) => msg,
        }
    }

    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            ErrorKind::Io(ref err) => Some(err),
            ErrorKind::DeserializeAnyNotSupported => None,
            ErrorKind::InvalidBoolEncoding(_) => None,
            ErrorKind::NotEnoughBytes => None,
            ErrorKind::Custom(_) => None,
            ErrorKind::NotSupportedFormat(_) => None,
        }
    }
}



impl Display for ErrorKind {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        match *self {
            ErrorKind::Io(ref ioerr) => write!(fmt, "io error: {}", ioerr),
            ErrorKind::DeserializeAnyNotSupported => write!(
                fmt,
                "Kafka does not support the serde::Deserializer::deserialize_any method"
            ),
            ErrorKind::InvalidBoolEncoding(b) => {
                write!(fmt, "{}, expected 0 or 1, found {}", self.description(), b)
            },
            ErrorKind::NotEnoughBytes => {
                write!(fmt, "not enought bytes")
            }
            ErrorKind::NotSupportedFormat(_) => {
                write!(fmt, "{}, not supported", self.description())
            }
            ErrorKind::Custom(ref s) => s.fmt(fmt),
        }
    }
}

impl From<IoError> for Error {
    fn from(err:  IoError) -> Error {
        ErrorKind::Io(err).into()
    }
}

impl DeError for Error {
    fn custom<T: Display>(desc: T) -> Error {
        ErrorKind::Custom(desc.to_string()).into()
    }
}
