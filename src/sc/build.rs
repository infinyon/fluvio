fn main() {
    // Copy VERSION file. Do not fail e.g. when built via `cargo publish`
    println!("cargo:rerun-if-changed=../../VERSION");
    let _ = std::fs::copy("../../VERSION", "./src/VERSION");
}
