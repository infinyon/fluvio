#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/wrong_direction.rs");
    t.compile_fail("tests/ui/wrong_input_len.rs");
    t.compile_fail("tests/ui/wrong_rust_type.rs");
    t.compile_fail("tests/ui/not_async.rs");
    t.compile_fail("tests/ui/invalid_config_type.rs");
    t.compile_fail("tests/ui/config_input_self.rs");
    t.compile_fail("tests/ui/config_use_reserved_name_fluvio.rs");
    t.compile_fail("tests/ui/config_use_reserved_name_transforms.rs");
    t.compile_fail("tests/ui/struct_without_config.rs");
}
