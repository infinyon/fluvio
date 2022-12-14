#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/sink.rs");
    t.compile_fail("tests/ui/wrong_direction.rs");
    t.compile_fail("tests/ui/wrong_input_len.rs");
    t.compile_fail("tests/ui/not_async.rs");
}
