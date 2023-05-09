#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("ui-test/ui/wrong_direction.rs");
    t.compile_fail("ui-test/ui/wrong_input_len.rs");
    t.compile_fail("ui-test/ui/wrong_rust_type.rs");
    t.compile_fail("ui-test/ui/not_async.rs");
    t.compile_fail("ui-test/ui/invalid_config_type.rs");
    t.compile_fail("ui-test/ui/config_input_self.rs");
    t.compile_fail("ui-test/ui/config_use_reserved_name_fluvio.rs");
    t.compile_fail("ui-test/ui/config_use_reserved_name_transforms.rs");
    t.compile_fail("ui-test/ui/struct_without_config.rs");
}
