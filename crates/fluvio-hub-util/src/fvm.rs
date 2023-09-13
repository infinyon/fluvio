///! Fluvio Version Manager (FVM) type definitions.

/// Available Rust Targets for Fluvio.
///
/// Refer: https://github.com/infinyon/fluvio/blob/f2c49e126c771d58d24d5f5cb0282a6aaa6b23ca/.github/workflows/ci.yml#L141
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum RustTarget {
    /// arm-unknown-linux-gnueabihf
    ArmUnknownLinuxGnueabihf,
    /// armv7-unknown-linux-gnueabihf
    Armv7UnknownLinuxGnueabihf,
    /// x86_64-apple-darwin
    X86_64AppleDarwin,
    /// aarch64-apple-darwin
    Aarch64AppleDarwin,
    /// x86_64-pc-windows-gnu
    X86_64PcWindowsGnu,
}

impl Display for RustTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ArmUnknownLinuxGnueabihf => write!(f, "arm-unknown-linux-gnueabihf"),
            Self::Armv7UnknownLinuxGnueabihf => write!(f, "armv7-unknown-linux-gnueabihf"),
            Self::X86_64AppleDarwin => write!(f, "x86_64-apple-darwin"),
            Self::Aarch64AppleDarwin => write!(f, "aarch64-apple-darwin"),
            Self::X86_64PcWindowsGnu => write!(f, "x86_64-pc-windows-gnu"),
        }
    }
}

impl RustTarget {
    pub fn maybe_form_str(s: &str) -> Option<Self> {
        match s {
            "arm-unknown-linux-gnueabihf" => Some(Self::ArmUnknownLinuxGnueabihf),
            "armv7-unknown-linux-gnueabihf" => Some(Self::Armv7UnknownLinuxGnueabihf),
            "x86_64-apple-darwin" => Some(Self::X86_64AppleDarwin),
            "aarch64-apple-darwin" => Some(Self::Aarch64AppleDarwin),
            "x86_64-pc-windows-gnu" => Some(Self::X86_64PcWindowsGnu),
            _ => None,
        }
    }
}

/// Artifact download URL
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Artifact {
    name: String,
    download_url: Url,
}

/// Fluvio Version Manager Package for a specific architecture and version.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PackageSet {
    pub version: String,
    pub arch: RustTarget,
    pub artifacts: Vec<Artifact>,
}
