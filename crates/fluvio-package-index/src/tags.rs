use std::fmt;
use crate::Error;

/// Represents names that may be used for Tags
///
/// This includes any ascii string that does not contain a `/`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagName(String);

impl AsRef<str> for TagName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for TagName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.is_ascii() {
            return Err(Error::InvalidTagName(format!(
                "tag name must be ascii, got '{s}'"
            )));
        }
        if s.contains('/') {
            return Err(Error::InvalidTagName(format!(
                "tag name may not contain '/', got '{s}'"
            )));
        }

        Ok(Self(s.to_string()))
    }
}

impl fmt::Display for TagName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
