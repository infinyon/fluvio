//!
//! # Output to file
//!
//! Takes code and pushes to an output directory.
//! Any existing file with colliding name, will be replaced.
//!

use std::fs::metadata;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;

use inflector::Inflector;

// -----------------------------------
// Macros
// -----------------------------------

macro_rules! make_rust_filename {
    ($filename:expr) => {
        &format!("{}.rs", $filename.to_snake_case());
    };
}

// -----------------------------------
// Implementation
// -----------------------------------

/// Take Request/Reponse code, format to file and inject into directory
pub fn code_to_output_file(file: &mut File, code: String) -> Result<(),IoError> {
    file.write_all(code.as_bytes())?;

    Ok(())
}

// Generate a file pointer form a filename
pub fn file_from_name<P>(filename: &P) -> Result<File, IoError>
where
    P: AsRef<Path>,
{
    match open_file(filename) {
        Ok(file) => Ok(file),
        Err(err) => Err(IoError::new(
            ErrorKind::InvalidData,
            format!("{} - {}", filename.as_ref().display(), err),
        )),
    }
}

/// Create and open file
pub fn open_file<P>(file_path: P) -> Result<File,IoError>
where
    P: AsRef<Path>,
{
    Ok(OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)?)
}

/// Create and open file
pub fn make_file_path(dir: &str, filename: &str) -> Result<String,IoError> {
    // check if exists
    metadata(dir)?;

    // construct file
    let mut target_file = PathBuf::new();
    target_file.push(dir);
    target_file.push(make_rust_filename!(filename));

    Ok(target_file.to_string_lossy().to_string())
}

// Generate a file pointer form directory and filename
pub fn make_file_from_dir(dir: &str, filename: &str) -> Result<File, IoError> {
    match make_file_path(dir, &filename) {
        Ok(file_path) => match open_file(&file_path) {
            Ok(file) => Ok(file),
            Err(err) => Err(IoError::new(
                ErrorKind::InvalidData,
                format!("{} - {}", filename, err),
            )),
        },
        Err(err) => Err(IoError::new(
            ErrorKind::InvalidData,
            format!("{} - {}", filename, err),
        )),
    }
}
