fn main() {
    println!(
        "cargo:rustc-env=PACKAGE_TARGET={}",
        std::env::var("TARGET").unwrap()
    );
}
