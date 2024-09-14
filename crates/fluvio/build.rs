fn main() {
    // Copy VERSION file. Do not fail e.g. when built via `cargo publish`
    if let Ok(verpath) = std::fs::canonicalize("./VERSION") {
        if verpath.exists() {
            println!("cargo:rerun-if-changed=./VERSION");
        }
    }
    println!("cargo:rerun-if-changed=build.rs");
}
