mod basic;

use std::fmt::Display;

pub use basic::BasicVersionCmd;

use current_platform::CURRENT_PLATFORM;
use comfy_table::Table;
use sha2::{Digest, Sha256};
use sysinfo::SystemExt;

/// Retrieves target platform details
///
/// # Example
///
/// ```ignore
/// current_platform(); // "aarch64-apple-darwin"
/// ```
///
#[inline]
pub fn current_platform() -> &'static str {
    CURRENT_PLATFORM
}

/// Read CLI and compute its sha256
pub fn calc_sha256() -> Option<String> {
    let path = std::env::current_exe().ok()?;
    let bin = std::fs::read(path).ok()?;
    let mut hasher = Sha256::new();

    hasher.update(bin);

    let bin_sha256 = hasher.finalize();
    Some(format!("{:x}", &bin_sha256))
}

/// Retrieves OS details
pub fn os_info() -> Option<String> {
    let sys = sysinfo::System::new_all();

    let info = format!(
        "{} {} (kernel {})",
        sys.name()?,
        sys.os_version()?,
        sys.kernel_version()?,
    );

    Some(info)
}

/// A helper struct for printing Fluvio version information in a standardised
/// format across all Fluvio Products.
///
/// # Example
///
/// ```ignore
/// let mut fluvio_version_printer = FluvioVersionPrinter::new("fluvio", "0.7.0");
///
/// fluvio_version_printer.append_extra("Release Channel", "stable");
/// fluvio_version_printer.append_extra("Git Commit", "abcdefg");
/// fluvio_version_printer.append_extra("OS Details", "Linux 5.4.0-42-generic (kernel 4.19.76-linuxkit)");
/// println!("{}", fluvio_version_printer);
/// ```
///
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FluvioVersionPrinter {
    name: String,
    version: String,
    extra: Vec<(String, String)>,
}

impl FluvioVersionPrinter {
    pub fn new(name: &str, version: &str) -> Self {
        Self {
            name: name.to_string(),
            version: version.to_string(),
            extra: vec![],
        }
    }

    fn arch(&self) -> String {
        current_platform().to_string()
    }

    fn sha256(&self) -> Option<String> {
        calc_sha256()
    }

    pub fn append_extra(&mut self, key: impl AsRef<str>, value: impl AsRef<str>) {
        self.extra
            .push((key.as_ref().to_string(), value.as_ref().to_string()));
    }
}

#[cfg(feature = "serde")]
impl FluvioVersionPrinter {
    pub fn to_json(&self) -> String {
        serde_json::to_json(&self)
    }
}

impl Display for FluvioVersionPrinter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut table = Table::new();

        // Spaces are relevant here, don't trim them
        table.load_preset("        :          ");
        table.add_row(vec![format!("{}", self.name), self.version.to_string()]);
        table.add_row(vec![format!("{} Arch", self.name), self.arch()]);

        if let Some(sha256) = self.sha256() {
            table.add_row(vec![format!("{} SHA256", self.name), sha256]);
        }

        for (key, value) in &self.extra {
            table.add_row(vec![key, value]);
        }

        write!(f, "{}", table)
    }
}

#[cfg(test)]
mod test {
    use super::FluvioVersionPrinter;

    #[test]
    fn version_output_as_table() {
        let mut fluvio_version_printer = FluvioVersionPrinter::new("Fluvio CLI", "0.7.0");

        fluvio_version_printer.append_extra("Release Channel", "stable");
        fluvio_version_printer.append_extra("Git Commit", "abcdefg");
        fluvio_version_printer.append_extra(
            "OS Details",
            "Linux 5.4.0-42-generic (kernel 4.19.76-linuxkit)",
        );

        let lines = format!("{}", fluvio_version_printer)
            .lines()
            .map(String::from)
            .collect::<Vec<String>>();

        assert!(lines[0].contains("Fluvio CLI"));
        assert!(lines[0].contains("0.7.0"));
        assert!(lines[1].contains("Fluvio CLI Arch"));
        assert!(lines[1].contains(&fluvio_version_printer.arch()));
        assert!(lines[2].contains("Fluvio CLI SHA256"));

        // Extras
        assert!(lines[3].contains("Release Channel"));
        assert!(lines[3].contains("stable"));

        assert!(lines[4].contains("Git Commit"));
        assert!(lines[4].contains("abcdefg"));

        assert!(lines[5].contains("OS Details"));
        assert!(lines[5].contains("Linux 5.4.0-42-generic (kernel 4.19.76-linuxkit)"));
    }
}
