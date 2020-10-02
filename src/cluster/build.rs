use std::fs;

fn main() {
    println!("cargo:rerun-if-changed=../../VERSION");
    println!("cargo:rerun-if-changed=build.rs");
    fs::copy("../../VERSION", "src/VERSION").expect("version copy");
}
