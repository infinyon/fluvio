//!
//! # Kafka Json Spec to Rust code
//!
//! Takes a Kafka json spec file and generates Rust data structures
//!

mod constants;
mod file_content;
mod file_pairs;
mod file_to_json;
mod json_to_msg;
mod spec_msg;

pub mod check_keys;
pub mod format_code;
pub mod generate_code;
pub mod output_to_file;
