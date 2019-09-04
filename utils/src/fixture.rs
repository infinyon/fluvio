use log::info;
use std::fs::remove_dir_all;
use std::fs::remove_file;
use std::fs::create_dir_all;
use std::io;
use std::path::Path;


pub fn ensure_clean_dir<P>(dir_path: P) where P: AsRef<Path> {
    let path = dir_path.as_ref();
    match remove_dir_all(path) {
        Ok(_) => info!("removed dir: {}", path.display()),
        Err(_) => info!("unable to delete dir: {}", path.display()),
    }
}

pub fn ensure_new_dir<P>(path: P) -> Result<(),io::Error> where P: AsRef<Path> {
    let dir_path = path.as_ref();
    ensure_clean_dir(dir_path);
    create_dir_all(dir_path)
}

// remove existing file
pub fn ensure_clean_file<P>(path: P) where P: AsRef<Path> {
    let log_path = path.as_ref();
    if let Ok(_) = remove_file(log_path) {
        info!("remove existing file: {}", log_path.display());
    } else {
        info!("there was no existing file: {}", log_path.display());
    }
}