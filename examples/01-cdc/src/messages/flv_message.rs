use serde::{Deserialize, Serialize};

use crate::messages::{BinLogMessage, BnFile, Operation};

#[derive(Serialize, Deserialize, Debug)]
pub struct FluvioMessage {
    pub uri: String,
    pub sequence: u64,
    pub bn_file: BnFile,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,

    pub operation: Operation,
}

impl FluvioMessage {
    pub fn new(bn_message: BinLogMessage, sequence: u64) -> Self {
        Self {
            uri: bn_message.uri,
            sequence,
            bn_file: bn_message.bn_file,
            columns: bn_message.columns,
            operation: bn_message.operation,
        }
    }
}
