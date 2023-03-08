#[test]
fn ui() {
    let t = trybuild::TestCases::new();

    t.compile_fail("tests/ui/missing_tag_annotation.rs");
}
