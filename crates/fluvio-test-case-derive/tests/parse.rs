#[test]
fn derive_ui() {
    let t = trybuild::TestCases::check_only();

    t.pass("ui-tests/pass_*.rs");
    t.compile_fail("ui-tests/fail_*.rs");
}
