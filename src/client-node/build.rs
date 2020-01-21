fn main() {
    println!("cargo:rustc-cdylib-link-arg=-undefined");
    println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
}