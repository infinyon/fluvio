#[test]
fn ui() {
    let t = trybuild::TestCases::new();

    t.compile_fail("tests/ui/missing_fluvio_annotation.rs");
    t.compile_fail("tests/ui/missing_fluvio_annotation_with_tag.rs");
}
