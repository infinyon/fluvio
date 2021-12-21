use fluvio_smartmodule::SmartOpt;
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;
use std::collections::BTreeMap;
use std::convert::TryInto;

#[derive(Default, SmartOpt, PartialEq, Debug)]
pub struct TestStruct {
    s: String,
    i: i32,
    o_i: Option<i32>,
    f: f64,
    b: bool,
}

#[test]
fn smart_opt() {
    let mut b = BTreeMap::new();
    b.insert("s".to_owned(), "aaa".to_owned());
    b.insert("i".to_owned(), "10".to_owned());
    b.insert("o_i".to_owned(), "100".to_owned());
    b.insert("b".to_owned(), "true".to_owned());
    b.insert("f".to_owned(), "2.7".to_owned());

    let params: SmartModuleExtraParams = b.into();

    let t: TestStruct = params.try_into().expect("Unable to covert");

    assert_eq!(
        t,
        TestStruct {
            s: "aaa".to_owned(),
            i: 10,
            o_i: Some(100),
            b: true,
            f: 2.7
        }
    )
}
