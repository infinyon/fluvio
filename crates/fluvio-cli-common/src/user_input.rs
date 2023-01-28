use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;

use anyhow::{self, Result};

use fluvio_protocol::record::{Record, RecordData};
use fluvio_protocol::bytes::Bytes;

pub enum UserInputType {
    Text { key: Option<Bytes>, data: Bytes },
    File { key: Option<Bytes>, path: PathBuf },
    FileByLine { key: Option<Bytes>, path: PathBuf },
}

#[derive(Debug, Default)]
pub struct UserInputRecords {
    key: Option<Bytes>,
    data: Vec<RecordData>,
    size: usize,
}

impl UserInputRecords {
    pub fn key(&self) -> Option<Bytes> {
        self.key.clone()
    }

    pub fn data(&self) -> Vec<RecordData> {
        self.data.clone()
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}
impl From<UserInputRecords> for RecordData {
    fn from(input: UserInputRecords) -> Self {
        let mut data: Vec<u8> = Vec::new();

        for r in input.data {
            let v: Vec<u8> = r.as_ref().to_vec();
            data.extend(v.iter());
        }

        RecordData::from(data)
    }
}

impl From<UserInputRecords> for Vec<Record> {
    fn from(input: UserInputRecords) -> Self {
        if let Some(key) = input.key {
            input
                .data
                .into_iter()
                .map(|r| Record::new_key_value(key.clone(), r))
                .collect()
        } else {
            input.data.into_iter().map(Record::new).collect()
        }
    }
}

impl From<String> for UserInputRecords {
    fn from(input: String) -> Self {
        let data = RecordData::from(input.as_bytes());
        let size = data.len();
        UserInputRecords {
            key: None,
            data: vec![data],
            size,
        }
    }
}

impl TryFrom<UserInputType> for UserInputRecords {
    type Error = anyhow::Error;
    fn try_from(input: UserInputType) -> Result<Self> {
        match input {
            UserInputType::Text { key, data } => Ok(UserInputRecords {
                key,
                size: data.len(),
                data: vec![RecordData::from(data)],
            }),
            UserInputType::File { key, path } => {
                let data: Vec<u8> = std::fs::read(path)?;
                let size = data.len();

                Ok(UserInputRecords {
                    key,
                    data: vec![RecordData::from(data)],
                    size,
                })
            }
            UserInputType::FileByLine { key, path } => {
                let file = File::open(path)?;
                let buf = io::BufReader::new(file).lines();
                let mut size = 0;

                let mut data: Vec<RecordData> = Vec::new();

                for line in buf.flatten() {
                    let l = line.as_bytes();

                    size += l.len();
                    data.push(RecordData::from(line));
                }

                Ok(UserInputRecords { key, data, size })
            }
        }
    }
}

#[cfg(test)]
mod file_tests {
    use super::{UserInputRecords, UserInputType, Bytes};
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn text_only() -> Result<(), ()> {
        let key = "hello123 🚅 👴 🍡 🎰 🛂 🔣 🏂 ⚗ 📰 🏸 🙇 🔊 🖊 🐝";
        let data = "datadata1234Ⓜ️ 🎉 ☣ 💯 🆑 🌠 🐌 📠 😅 🏹 🐺 ⏸ 7️⃣ 🎯";

        let d = UserInputRecords::try_from(UserInputType::Text {
            key: Some(Bytes::from(key)),
            data: Bytes::from(data),
        })
        .unwrap();

        assert_eq!(d.data().len(), 1);
        assert_eq!(d.len(), data.len());
        assert_eq!(std::str::from_utf8(d.key().as_ref().unwrap()).unwrap(), key);
        assert_eq!(std::str::from_utf8(d.data()[0].as_ref()).unwrap(), data);

        Ok(())
    }
    #[test]
    fn file_lines() -> Result<(), ()> {
        let mut file = NamedTempFile::new().unwrap();
        let data = vec!["123", "abc", "📼🍅🐊"];

        writeln!(file, "{}", data[0]).unwrap();
        writeln!(file, "{}", data[1]).unwrap();
        writeln!(file, "{}", data[2]).unwrap();

        let d = UserInputRecords::try_from(UserInputType::FileByLine {
            key: None,
            path: file.path().to_path_buf(),
        })
        .unwrap();

        assert_eq!(d.data().len(), 3);
        assert_eq!(d.len(), data.iter().map(|d| d.len()).sum::<usize>());
        assert_eq!(std::str::from_utf8(d.data()[0].as_ref()).unwrap(), data[0]);
        assert_eq!(std::str::from_utf8(d.data()[1].as_ref()).unwrap(), data[1]);
        assert_eq!(std::str::from_utf8(d.data()[2].as_ref()).unwrap(), data[2]);

        Ok(())
    }

    #[test]
    fn file_whole() -> Result<(), ()> {
        let mut file = NamedTempFile::new().unwrap();

        let data = vec!["123", "abc", "📼🍅🐊"];

        writeln!(file, "{}", data[0]).unwrap();
        writeln!(file, "{}", data[1]).unwrap();
        writeln!(file, "{}", data[2]).unwrap();

        let d = UserInputRecords::try_from(UserInputType::File {
            key: None,
            path: file.path().to_path_buf(),
        })
        .unwrap();

        assert_eq!(d.data().len(), 1);
        assert_eq!(
            d.len(),
            data.iter().map(|d| format!("{d}\n").len()).sum::<usize>()
        );
        assert_eq!(
            std::str::from_utf8(d.data()[0].as_ref()).unwrap(),
            format!("{}\n", data.join("\n"))
        );

        Ok(())
    }
}
