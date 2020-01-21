
/// configure linker to generate node.js dynamic library
pub fn configure() {
    println!("cargo:rustc-cdylib-link-arg=-undefined");
    println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
}