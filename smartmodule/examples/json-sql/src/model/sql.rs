//! A copy of https://github.com/infinyon/sql-connector/blob/99beb852e79df81fdecd23edf2dce0517a7b5111/crates/fluvio-model-sql/src/lib.rs

use serde::Deserialize;
use serde::Serialize;

/// Top-level list of supported operations in the SQL model.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Operation {
    Insert { table: String, values: Vec<Value> },
}

/// Value with SQL column name and supported SQL type.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct Value {
    pub column: String,
    pub raw_value: String,
    #[serde(rename = "type")]
    pub type_: Type,
}

/// Supported SQL data types.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Type {
    Bool,
    Char,

    SmallInt,
    Int,
    BigInt,

    Float,
    DoublePrecision,

    Text,
    Bytes,

    Numeric,

    Timestamp,
    Date,
    Time,

    Uuid,

    Json,
}
