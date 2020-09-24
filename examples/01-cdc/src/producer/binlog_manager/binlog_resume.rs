use std::fmt;

use crate::messages::BnFile;

#[derive(Debug)]
pub struct Resume {
    file: String,
    offset: Option<u64>,
}

impl fmt::Display for Resume {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let offset = if let Some(offset) = self.offset {
            offset
        } else {
            0
        };
        write!(
            f,
            "Resume listening at '{:?}' [offset {:?}]",
            self.file, offset
        )?;
        Ok(())
    }
}

impl Resume {
    pub fn new(bn_file: Option<BnFile>) -> Option<Self> {
        if let Some(bn_file) = bn_file {
            Some(Self {
                file: bn_file.file_name,
                offset: bn_file.offset,
            })
        } else {
            None
        }
    }

    pub fn file(&self) -> String {
        self.file.clone()
    }

    pub fn offset(&self) -> Option<u64> {
        self.offset
    }
}
