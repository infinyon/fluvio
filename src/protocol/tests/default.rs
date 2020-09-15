use fluvio_protocol::derive::FluvioDefault;

#[derive(FluvioDefault, Debug)]
struct TestRecord {
    value: i8,
    value2: i8,
    #[fluvio(default = "4")]
    value3: i8,
    #[fluvio(default = "-1")]
    value4: i16,
}

#[test]
fn test_default() {
    let record = TestRecord::default();
    assert_eq!(record.value3, 4);
    assert_eq!(record.value4, -1);
}
