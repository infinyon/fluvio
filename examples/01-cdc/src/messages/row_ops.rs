use serde::{Deserialize, Serialize};

use crate::messages::Value;

#[derive(Serialize, Deserialize, Debug)]
pub struct WriteRows {
    pub rows: Vec<Cols>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateRows {
    pub rows: Vec<BeforeAfterCols>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeleteRows {
    pub rows: Vec<Cols>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Cols {
    pub cols: Vec<Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BeforeAfterCols {
    pub before_cols: Vec<Value>,
    pub after_cols: Vec<Value>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_write_rows() {
        let val = WriteRows {
            rows: vec![Cols {
                cols: vec![
                    Value::String("John".to_owned()),
                    Value::String("tttt".to_owned()),
                    Value::String("m".to_owned()),
                    Value::Date {
                        year: 2000,
                        month: 3,
                        day: 30,
                    },
                ],
            }],
        };
        let expected_result = "{\"rows\":[{\"cols\":[{\"String\":\"John\"},{\"String\":\"tttt\"},{\"String\":\"m\"},{\"Date\":{\"year\":2000,\"month\":3,\"day\":30}}]}]}";

        let json_data = serde_json::to_string(&val).unwrap();
        assert_eq!(json_data, expected_result.to_owned());
    }

    #[test]
    fn test_update_rows() {
        let val = UpdateRows {
            rows: vec![BeforeAfterCols {
                before_cols: vec![
                    Value::String("Jack".to_owned()),
                    Value::String("Peter".to_owned()),
                    Value::String("dog".to_owned()),
                    Value::String("m".to_owned()),
                    Value::Date {
                        year: 1999,
                        month: 3,
                        day: 30,
                    },
                ],
                after_cols: vec![
                    Value::String("Jack".to_owned()),
                    Value::String("Peter".to_owned()),
                    Value::String("dog".to_owned()),
                    Value::String("m".to_owned()),
                    Value::Date {
                        year: 1989,
                        month: 8,
                        day: 31,
                    },
                ],
            }],
        };
        let expected_result = "{\"rows\":[{\"before_cols\":[{\"String\":\"Jack\"},{\"String\":\"Peter\"},{\"String\":\"dog\"},{\"String\":\"m\"},{\"Date\":{\"year\":1999,\"month\":3,\"day\":30}}],\"after_cols\":[{\"String\":\"Jack\"},{\"String\":\"Peter\"},{\"String\":\"dog\"},{\"String\":\"m\"},{\"Date\":{\"year\":1989,\"month\":8,\"day\":31}}]}]}";

        let json_data = serde_json::to_string(&val).unwrap();
        assert_eq!(json_data, expected_result.to_owned());
    }

    #[test]
    fn test_delete_rows() {
        let val = DeleteRows {
            rows: vec![Cols {
                cols: vec![
                    Value::String("Puffball".to_owned()),
                    Value::String("Diane".to_owned()),
                    Value::String("hamster".to_owned()),
                    Value::String("f".to_owned()),
                    Value::Date {
                        year: 1999,
                        month: 3,
                        day: 30,
                    },
                    Value::Null,
                ],
            }],
        };
        let expected_result = "{\"rows\":[{\"cols\":[{\"String\":\"Puffball\"},{\"String\":\"Diane\"},{\"String\":\"hamster\"},{\"String\":\"f\"},{\"Date\":{\"year\":1999,\"month\":3,\"day\":30}},\"Null\"]}]}";

        let json_data = serde_json::to_string(&val).unwrap();

        assert_eq!(json_data, expected_result.to_owned());
    }
}
