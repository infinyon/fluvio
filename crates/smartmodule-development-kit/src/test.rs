use std::{collections::BTreeMap, path::PathBuf};

use std::fmt::Debug;
use std::fs::File;
use std::io::{self, BufRead};

use clap::Parser;
use anyhow::{self, Result};
use fluvio_smartengine::metrics::SmartModuleChainMetrics;
use tracing::debug;

use fluvio::RecordKey;
use fluvio_protocol::record::Record;
use fluvio_smartengine::{SmartEngine, SmartModuleChainBuilder, SmartModuleConfig};
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

use crate::package::{PackageInfo, PackageOption};

/// Test SmartModule
#[derive(Debug, Parser)]
pub struct TestOpt {
    /// Provide test input with this flag
    #[clap(long, group = "TestInput")]
    text: Option<String>,

    /// Path to test file. Default: Read file line by line
    #[clap(long, groups = ["TestInput", "TestFile"])]
    file: Option<PathBuf>,

    /// Read the file as single record
    #[clap(long, action, requires = "TestFile")]
    file_single: bool,

    /// Key to use with the test record(s)
    key: Option<String>,

    // TODO read in from topic, delete for PR
    // topic
    #[clap(flatten)]
    package: PackageOption,

    /// Optional wasm file path
    #[clap(long)]
    wasm_file: Option<PathBuf>,

    /// (Optional) Extra input parameters passed to the smartmodule module.
    /// They should be passed using key=value format
    /// Eg. fluvio consume topic-name --filter filter.wasm -e foo=bar -e key=value -e one=1
    #[clap(
        short = 'e',
        long= "params",
        value_parser=parse_key_val,
        number_of_values = 1
    )]
    params: Vec<(String, String)>,
}

fn parse_key_val(s: &str) -> Result<(String, String)> {
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow::anyhow!(format!("invalid KEY=value: no `=` found in `{}`", s)))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

impl TestOpt {
    pub(crate) fn process(self) -> Result<()> {
        debug!("starting smart module test");

        let raw = match &self.wasm_file {
            Some(wasm_file) => crate::read_bytes_from_path(wasm_file)?,
            None => PackageInfo::from_options(&self.package)
                .map_err(|e| anyhow::anyhow!(e))?
                .read_bytes()?,
        };

        let param: BTreeMap<String, String> = self.params.into_iter().collect();

        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();
        chain_builder.add_smart_module(
            SmartModuleConfig::builder().params(param.into()).build()?,
            raw,
        );

        debug!("SmartModule chain created");

        let mut chain = chain_builder.initialize(&engine)?;

        let key = self.key;

        let test_record: UserInputData = if let Some(data) = self.text {
            UserInputData::try_from(UserInputType::Text { key, data })?
        } else if let Some(test_file_path) = &self.file {
            let path = test_file_path.to_path_buf();
            if self.file_single {
                UserInputData::try_from(UserInputType::File { key, path })?
            } else {
                UserInputData::try_from(UserInputType::FileByLine { key, path })?
            }
        } else {
            return Err(anyhow::anyhow!("No valid input provided"));
        };
        debug!(len = &test_record.len(), "input data");

        let metrics = SmartModuleChainMetrics::default();
        let output = chain.process(SmartModuleInput::try_from(test_record.data())?, &metrics)?;

        println!("{:?} records outputed", output.successes.len());
        for output_record in output.successes {
            let output_value = output_record.value.as_str()?;
            println!("{}", output_value);
        }

        Ok(())
    }
}

enum UserInputType {
    Text { key: Option<String>, data: String },
    File { key: Option<String>, path: PathBuf },
    FileByLine { key: Option<String>, path: PathBuf },
}

#[derive(Debug, Default)]
struct UserInputData {
    data: Vec<Record>,
    size: usize,
}

impl UserInputData {
    pub fn data(&self) -> Vec<Record> {
        self.data.clone()
    }

    pub fn len(&self) -> usize {
        self.size
    }
}

impl From<String> for UserInputData {
    fn from(input: String) -> Self {
        let input_bytes = input.as_bytes().to_vec();
        let size = input_bytes.len();
        UserInputData {
            data: vec![Record::new_key_value(RecordKey::NULL, input_bytes)],
            size,
        }
    }
}

impl TryFrom<UserInputType> for UserInputData {
    type Error = anyhow::Error;
    fn try_from(input: UserInputType) -> Result<Self> {
        match input {
            UserInputType::Text { key, data } => {
                let key = if let Some(k) = key {
                    RecordKey::from(k)
                } else {
                    RecordKey::NULL
                };
                Ok(UserInputData {
                    size: data.len(),
                    data: vec![Record::new_key_value(key, data)],
                })
            }
            UserInputType::File { key, path } => {
                let key = if let Some(k) = key {
                    RecordKey::from(k)
                } else {
                    RecordKey::NULL
                };
                let f: Vec<u8> = std::fs::read(path)?;
                let size = f.len();

                Ok(UserInputData {
                    data: vec![Record::new_key_value(key, f)],
                    size,
                })
            }
            UserInputType::FileByLine { key, path } => {
                let file = File::open(path)?;
                let buf = io::BufReader::new(file).lines();
                let mut size = 0;

                let mut data: Vec<Record> = Vec::new();

                for line in buf.flatten() {
                    let key = if let Some(k) = key.clone() {
                        RecordKey::from(k)
                    } else {
                        RecordKey::NULL
                    };
                    let l = line.as_bytes();

                    size += l.len();
                    data.push(Record::new_key_value(key, line.as_bytes()));
                }

                Ok(UserInputData { data, size })
            }
        }
    }
}

#[cfg(test)]
mod file_tests {
    use super::{UserInputData, UserInputType};
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn text_only() -> Result<(), ()> {
        let key = "hello 🚅 👴 🍡 🎰 🛂 🔣 🏂 ⚗ 📰 🏸 🙇 🔊 🖊 🐝";
        let data = "Ⓜ️ 🎉 ☣ 💯 🆑 🌠 🐌 📠 😅 🏹 🐺 ⏸ 7️⃣ 🎯";

        let d = UserInputData::try_from(UserInputType::Text {
            key: Some(key.to_string()),
            data: data.to_string(),
        })
        .unwrap();

        let r = d.data();

        assert_eq!(r.len(), 1);
        assert_eq!(r[0].key().unwrap().as_str().unwrap(), key);
        assert_eq!(r[0].value().to_string(), data.to_string());

        Ok(())
    }
    #[test]
    fn file_lines() -> Result<(), ()> {
        let mut file = NamedTempFile::new().unwrap();
        let data = vec!["123", "abc", "📼🍅🐊"];

        writeln!(file, "{}", data[0]).unwrap();
        writeln!(file, "{}", data[1]).unwrap();
        writeln!(file, "{}", data[2]).unwrap();

        let d = UserInputData::try_from(UserInputType::FileByLine {
            key: None,
            path: file.path().to_path_buf(),
        })
        .unwrap();

        let r = d.data();

        assert_eq!(r.len(), 3);
        assert_eq!(r[0].value().to_string(), data[0].to_string());
        assert_eq!(r[1].value().to_string(), data[1].to_string());
        assert_eq!(r[2].value().to_string(), data[2].to_string());

        Ok(())
    }

    #[test]
    fn file_whole() -> Result<(), ()> {
        // Create a file inside of `std::env::temp_dir()`.
        let mut file = NamedTempFile::new().unwrap();

        let data = vec!["123", "abc", "📼🍅🐊"];

        writeln!(file, "{}", data[0]).unwrap();
        writeln!(file, "{}", data[1]).unwrap();
        writeln!(file, "{}", data[2]).unwrap();

        let d = UserInputData::try_from(UserInputType::File {
            key: None,
            path: file.path().to_path_buf(),
        })
        .unwrap();

        let r = d.data();

        assert_eq!(r.len(), 1);
        assert_eq!(r[0].value().to_string(), format!("{}\n", data.join("\n")));

        Ok(())
    }
}
