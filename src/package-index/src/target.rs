use std::fmt;
use serde::{Serialize, Deserialize};
use crate::Error;
use std::borrow::Cow;

const PACKAGE_TARGET: &str = env!("PACKAGE_TARGET");

/// Detects the target triple of the current build and returns
/// the name of a compatible build target on packages.fluvio.io.
///
/// Returns `Some(Target)` if there is a compatible target, or
/// `None` if this target is unsupported or has no compatible target.
pub fn package_target() -> Result<Target, Error> {
    let target = PACKAGE_TARGET.parse()?;
    Ok(target)
}

/// An object representing a specific build target for an artifact
/// being managed by fluvio-index.
///
/// This type is generally constructed using `FromStr` via the
/// `parse` method.
///
/// # Example
///
/// ```
/// # use fluvio_index::Target;
/// let target: Target = "x86_64-unknown-linux-musl".parse().unwrap();
/// ```
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Target(Cow<'static, str>);

#[allow(non_upper_case_globals)]
impl Target {
    // These constants are from back when `Target` was an enum.
    // Their variants are now constants, so constructors should not have broken
    pub const X86_64AppleDarwin: Target = Target(Cow::Borrowed("x86_64-apple-darwin"));
    pub const X86_64UnknownLinuxMusl: Target = Target(Cow::Borrowed("x86_64-unknown-linux-musl"));
    pub const ALL_TARGETS: &'static [Target] =
        &[Target::X86_64AppleDarwin, Target::X86_64UnknownLinuxMusl];

    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::str::FromStr for Target {
    type Err = Error;

    /// When parsing from a string, here is the chance to make any
    /// edits or to collapse multiple target names into one. An
    /// example of this is how we transform the target name
    /// `x86_64-unknown-linux-gnu` into `x86_64-unknown-linux-musl`.
    ///
    /// Additionally, if there are any "target strings" that we
    /// know for a fact that we cannot support, we can identify
    /// those strings here and manually reject them in order to
    /// prevent downstream tooling from incorrectly allowing those
    /// targets.
    ///
    /// All other target names should pass through unchanged.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let platform = match s {
            "x86_64-apple-darwin" => Self::X86_64AppleDarwin,
            "x86_64-unknown-linux-musl" => Self::X86_64UnknownLinuxMusl,
            "x86_64-unknown-linux-gnu" => Self::X86_64UnknownLinuxMusl,
            other => Self(Cow::Owned(other.to_owned())),
        };
        Ok(platform)
    }
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl<'de> Deserialize<'de> for Target {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        let me: Self = std::str::FromStr::from_str(&string).map_err(|e: Error| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Other(&e.to_string()),
                &"valid Target",
            )
        })?;
        Ok(me)
    }
}
