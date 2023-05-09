#[test]
fn ui() {
    let t = trybuild::TestCases::new();

    t.compile_fail("derive-test/ui/fail/*.rs");
    t.pass("derive-test/ui/pass/*.rs");
}
