pub use encoding::SmartStreamUserError;
pub use encoding::SmartStreamOutput;

pub type Result<T> = std::result::Result<T, SmartStreamUserError>;

mod encoding {
    use crate::record::Record;
    use fluvio_protocol::derive::{Encode, Decode};
    use std::fmt;

    /// A type used to capture and serialize errors from within a SmartStream
    #[derive(Debug, Default, Encode, Decode)]
    pub struct SmartStreamUserError {
        rendered: String,
    }

    impl fmt::Display for SmartStreamUserError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.rendered.fmt(f)
        }
    }

    impl<E> From<E> for SmartStreamUserError
    where
        E: Into<eyre::Error>,
    {
        fn from(err: E) -> Self {
            let report = err.into();
            Self {
                rendered: format!("{:?}", report),
            }
        }
    }

    /// A type used to return processed records and/or an error from a SmartStream
    #[derive(Debug, Default, Encode, Decode)]
    pub struct SmartStreamOutput {
        pub successes: Vec<Record>,
        pub error: Option<SmartStreamUserError>,
    }
}
