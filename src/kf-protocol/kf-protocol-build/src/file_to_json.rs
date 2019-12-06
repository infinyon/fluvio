//!
//! # File to Json
//!
//! Reads and validates that file is proper json
//!

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::PathBuf;
use std::process;

use serde_json::Value;
use serde_json::Result as JsonResult;

/// Convert string to json
pub fn file_to_json(file_name: &PathBuf) -> JsonResult<Value> {
    let v = serde_json::from_str(&file_to_clean_json(&file_name))?;
    Ok(v)
}

/// Reads file and removes comments for clean json
fn file_to_clean_json(file_name: &PathBuf) -> String {
    // Access file
    let f = match File::open(file_name) {
        Ok(f) => f,
        Err(err) => {
            eprintln!("Error: {}", err);
            process::exit(1);
        }
    };
    let file = BufReader::new(&f);

    // strip comments & collect everything else
    let mut result = String::new();
    for line in file.lines() {
        if let Ok(text) = line {
            let raw = text.trim_start();
            if raw.len() >= 2 && &raw[..2] == "//" {
                continue;
            }
            result.push_str(&text);
        }
    }

    result
}
