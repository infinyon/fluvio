//!
//! # File Pairs
//!
//! Data structure to cache a pair of request/response files
//!
use std::fs;
use std::io::Error;
use std::io::ErrorKind;
use std::path::PathBuf;

#[derive(Debug, PartialEq)]
pub struct FilePair {
    pub req_file: PathBuf,
    pub res_file: PathBuf,
    pub filename: String,
}

#[derive(Debug, PartialEq)]
pub struct FilePairs {
    pub pairs: Vec<FilePair>,
}

// -----------------------------------
// Implementation
// -----------------------------------

impl FilePairs {
    /// Read directory and generate file pairs
    pub fn new(dir: &str) -> Result<FilePairs, Error> {
        let skip_files = vec!["RequestHeader".to_owned(), "ResponseHeader".to_owned()];
        let files = Self::files_in_dir(dir, &skip_files)?;
        let pairs = Self::make_pairs(files);
        Ok(FilePairs { pairs })
    }

    /// Read files in directory (subdirectories are skipped)
    fn files_in_dir(dir: &str, skip_files: &Vec<String>) -> Result<Vec<PathBuf>, Error> {
        let mut files: Vec<PathBuf> = vec![];
        let dir_files = match fs::read_dir(dir) {
            Ok(dir) => dir,
            Err(err) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("{} - {}", dir, err),
                ));
            }
        };

        for file_path in dir_files {
            let file = file_path?;
            let path = file.path();
            if path.is_dir() {
                println!("'{}' is directory... skipped", path.display());
                continue;
            }

            if let Some(filename) = path.file_stem() {
                let file_str = &filename
                    .to_os_string()
                    .into_string()
                    .unwrap_or("".to_owned());
                if skip_files.contains(file_str) {
                    println!("'{}' skipped... not implementeed", file_str);
                    continue;
                }
            }

            files.push(path);
        }

        Ok(files)
    }

    /// Take the files and make Request/Response file pairs
    fn make_pairs(files: Vec<PathBuf>) -> Vec<FilePair> {
        let mut file_pairs: Vec<FilePair> = vec![];
        let mut temp = files.clone();

        while temp.len() > 0 {
            let first_file_path = temp.remove(0);
            let first_file = if let Some(first) = first_file_path.file_stem() {
                match first.to_str() {
                    Some(file) => file,
                    None => continue,
                }
            } else {
                continue;
            };
            let filename = message_from_filename(&first_file.to_owned());

            if let Some((index, is_request)) = second_file_index(first_file.to_string(), &temp) {
                let second_file_path = temp.remove(index);

                // generate file pair
                let file_pair = if is_request {
                    FilePair {
                        req_file: second_file_path,
                        res_file: first_file_path,
                        filename: filename,
                    }
                } else {
                    FilePair {
                        req_file: first_file_path,
                        res_file: second_file_path,
                        filename: filename,
                    }
                };

                file_pairs.push(file_pair);
            }
        }

        file_pairs
    }
}

/// If Request file, looks-up the index of the Response file (or the reverese)
/// Returns the index of the file, or -1
fn second_file_index(first_file: String, files: &Vec<PathBuf>) -> Option<(usize, bool)> {
    let found_request: bool;

    // request or reponse
    let file_len = first_file.len();
    let request_len = first_file.find("Request").unwrap_or(file_len);
    let response_len = first_file.find("Response").unwrap_or(file_len);

    // generate second file
    let second_file = if file_len != request_len {
        found_request = false;

        if request_len == 0 {
            // Some files begin with Response
            let mut second_file = "Response".to_owned();
            second_file.push_str(&first_file["Request".len()..].to_owned());
            second_file
        } else {
            let mut second_file = first_file[..request_len].to_owned();
            second_file.push_str("Response");
            second_file
        }
    } else if file_len != response_len {
        found_request = true;

        if response_len == 0 {
            // Some files begin with Request
            let mut second_file = "Request".to_owned();
            second_file.push_str(&first_file["Response".len()..].to_owned());
            second_file
        } else {
            let mut second_file = first_file[..response_len].to_owned();
            second_file.push_str("Request");
            second_file
        }
    } else {
        println!("Invalid json file {}... skipped", first_file);
        return None;
    };

    // find second file
    let index = if let Some(index) = files.iter().position(|file| {
        if let Some(file_stem) = file.file_stem() {
            file_stem.to_str() == Some(&second_file)
        } else {
            false
        }
    }) {
        Some((index, found_request))
    } else {
        println!("{} - Cannot find >>> {}", first_file, second_file);
        None
    };

    // return index
    index
}

/// Takes a filename and returns the message type without Request/Response
fn message_from_filename(filename: &String) -> String {
    if let Some(req_idx) = filename.find("Request") {
        filename[0..req_idx].to_string()
    } else if let Some(res_idx) = filename.find("Response") {
        filename[0..res_idx].to_string()
    } else {
        filename.clone()
    }
}

// -----------------------------------
// Test Cases
// -----------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_files_in_dir() {
        let files = FilePairs::files_in_dir(&"./test-data".to_owned(), &vec![]);
        assert_eq!(files.is_ok(), true);
    }

    #[test]
    fn test_file_pairs_new() {
        let pairs = FilePairs::new(&"./test-data".to_owned());
        let expected_pairs = FilePairs {
            pairs: vec![FilePair {
                req_file: Path::new("./test-data/MetadataRequest.json").to_path_buf(),
                res_file: Path::new("./test-data/MetadataResponse.json").to_path_buf(),
                filename: "Metadata".to_owned(),
            }],
        };

        assert_eq!(pairs.is_ok(), true);
        assert_eq!(pairs.unwrap(), expected_pairs);
    }

    #[test]
    fn test_message_from_file() {
        assert_eq!(
            message_from_filename(&"MetadataRequest".to_string()),
            "Metadata".to_owned()
        );
        assert_eq!(
            message_from_filename(&"MetadataResponse".to_string()),
            "Metadata".to_owned()
        );
        assert_eq!(
            message_from_filename(&"SomeFile".to_string()),
            "SomeFile".to_owned()
        );
    }
}
