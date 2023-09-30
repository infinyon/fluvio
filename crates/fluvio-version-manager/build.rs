use std::env::var;

fn main() {
    println!(
        "cargo:rustc-env=TARGET={}",
        var("TARGET").expect("The `TARGET` environment variable is not present")
    );
}
