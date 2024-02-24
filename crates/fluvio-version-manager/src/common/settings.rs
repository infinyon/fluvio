use std::fs::{write, read_to_string};
use std::path::PathBuf;

use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};

use fluvio_hub_util::fvm::Channel;

use super::manifest::VersionManifest;
use super::workdir::fvm_workdir_path;

pub const SETTINGS_TOML_FILENAME: &str = "settings.toml";

/// The `settings.toml` is in charge of keeping track of the active version
/// through the default key, which holds the name of the directory under
/// `~/.fvm/pkgset/default/versions` for the desired default version.
#[derive(Debug, Serialize, Deserialize)]
pub struct Settings {
    /// The active `channel` for the Fluvio Installation
    pub channel: Option<Channel>,
    /// The specific version in use
    pub version: Option<String>,
}

impl Settings {
    /// Used to create an empty `settings.toml` file. This is used when the user
    /// installs FVM but no version is set yet.
    pub fn init() -> Result<Self> {
        let settings_path = Self::settings_file_path()?;

        if settings_path.exists() {
            return Err(Error::msg(format!(
                "Settings already exists on {}",
                settings_path.display()
            )));
        }

        let initial = Self {
            channel: None,
            version: None,
        };

        initial.save()?;
        tracing::debug!(?settings_path, "Created settings file with success");

        Ok(initial)
    }

    /// Opens the `settings.toml` file and parses it into a `Settings` struct.
    ///
    /// If the file doesn't exist, it will be created.
    pub fn open() -> Result<Self> {
        let settings_path = Self::settings_file_path()?;

        if !settings_path.exists() {
            Self::init()?;
        }

        let contents = read_to_string(settings_path)?;
        let settings: Settings = toml::from_str(&contents)?;

        Ok(settings)
    }

    /// Update settings file to keep track of active Fluvio Version
    pub fn update_from_manifest(&mut self, manifest: &VersionManifest) -> Result<()> {
        self.channel = Some(manifest.channel.to_owned());
        self.version = Some(manifest.version.to_string());
        self.save()?;

        Ok(())
    }

    /// Saves the `settings.toml` file to disk, overwriting the previous version
    fn save(&self) -> Result<()> {
        let settings_path = Self::settings_file_path()?;
        let settings_str = toml::to_string(&self)?;

        write(settings_path, settings_str)?;

        Ok(())
    }

    /// Retrieves the path to the `settings.toml` file for this host
    fn settings_file_path() -> Result<PathBuf> {
        let fvm_path = fvm_workdir_path()?;
        let settings_path = fvm_path.join(SETTINGS_TOML_FILENAME);

        Ok(settings_path)
    }
}

#[cfg(test)]
pub mod tests {
    use std::fs::{remove_file, read_to_string, create_dir, remove_dir_all};

    use semver::Version;

    use crate::common::{home_dir, manifest::VersionManifest};

    use super::*;

    pub fn create_fvm_dir() {
        let fvm_dir = fvm_workdir_path().unwrap();

        if !fvm_dir.exists() {
            create_dir(&fvm_dir).unwrap();
        }
    }

    pub fn delete_fvm_dir() {
        let fvm_dir = fvm_workdir_path().unwrap();

        if fvm_dir.exists() {
            remove_dir_all(&fvm_dir).unwrap();
        }
    }

    #[test]
    fn test_settings_file_path() {
        let settings_path =
            Settings::settings_file_path().expect("Failed to get settings.toml path");
        let home = home_dir().expect("Failed to get home directory");

        assert!(settings_path.is_absolute());
        assert!(settings_path.starts_with(home));
        assert!(settings_path.ends_with(SETTINGS_TOML_FILENAME));
    }

    #[test]
    fn creates_settings_file() {
        create_fvm_dir();

        let _settings = Settings::init().expect("Failed to create settings.toml file");
        let settings_path =
            Settings::settings_file_path().expect("Failed to get settings.toml path");

        assert!(
            settings_path.exists(),
            "the settings file should exist at this point"
        );

        remove_file(settings_path).expect("Failed to remove settings.toml file");
        delete_fvm_dir();
    }

    #[test]
    fn save_settings_file() {
        const WANT: &str = r#"channel = "stable"
version = "0.11.0"
"#;

        create_fvm_dir();

        let settings_path =
            Settings::settings_file_path().expect("Failed to get settings.toml path");
        let mut settings = Settings::init().expect("Failed to create settings.toml file");

        settings.channel = Some(Channel::Stable);
        settings.version = Some(Version::new(0, 11, 0).to_string());

        settings.save().expect("Failed to save settings.toml file");

        let settings_str =
            read_to_string(settings_path).expect("Failed to read settings.toml file");

        assert_eq!(settings_str, WANT);
        delete_fvm_dir();
    }

    #[test]
    fn updates_settings_file_contents() {
        const EXPECT_FIRST: &str = r#"channel = "stable"
version = "0.11.0"
"#;
        const EXPECT_SECOND: &str = r#"channel = "latest"
version = "0.12.0"
"#;
        create_fvm_dir();

        let settings_path =
            Settings::settings_file_path().expect("Failed to get settings.toml path");
        let mut settings = Settings::init().expect("Failed to create settings.toml file");

        settings.channel = Some(Channel::Stable);
        settings.version = Some(Version::new(0, 11, 0).to_string());
        settings.save().expect("Failed to save settings.toml file");

        let settings_str =
            read_to_string(&settings_path).expect("Failed to read settings.toml file");

        assert_eq!(settings_str, EXPECT_FIRST);

        settings.channel = Some(Channel::Latest);
        settings.version = Some(Version::new(0, 12, 0).to_string());
        settings.save().expect("Failed to save settings.toml file");

        let settings_str =
            read_to_string(&settings_path).expect("Failed to read settings.toml file");

        assert_eq!(settings_str, EXPECT_SECOND);

        delete_fvm_dir();
    }

    #[test]
    fn creates_settings_file_if_not_exists_on_open() {
        create_fvm_dir();

        let settings_path =
            Settings::settings_file_path().expect("Failed to get settings.toml path");

        assert!(
            !settings_path.exists(),
            "the settings file should not exist at this point"
        );

        let settings = Settings::open().expect("Failed to create settings.toml file");

        assert!(
            settings_path.exists(),
            "the settings file should exist at this point"
        );

        // The `settings` file wont have any values set after fresh creation
        assert!(settings.channel.is_none());
        assert!(settings.version.is_none());

        delete_fvm_dir();
    }

    #[test]
    fn updates_settings_toml_with_manifest_contents() {
        create_fvm_dir();

        const VERSION: &str = "0.10.0";

        let manifest = VersionManifest {
            channel: Channel::Stable,
            version: Version::parse(VERSION).unwrap(),
            contents: None,
        };

        let mut settings = Settings::open().unwrap();
        assert_eq!(settings.channel, None);
        assert_eq!(settings.version, None);

        settings.update_from_manifest(&manifest).unwrap();

        let settings = Settings::open().unwrap();
        assert_eq!(settings.channel, Some(Channel::Stable));
        assert_eq!(settings.version, Some(VERSION.to_string()));

        delete_fvm_dir();
    }
}
