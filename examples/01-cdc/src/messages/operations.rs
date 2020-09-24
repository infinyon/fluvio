use serde::{Deserialize, Serialize};

use crate::messages::{DeleteRows, UpdateRows, WriteRows};

#[derive(Serialize, Deserialize, Debug)]
pub enum Operation {
    Query(String),
    Add(WriteRows),
    Update(UpdateRows),
    Delete(DeleteRows),
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::messages::{Cols, Value};

    #[test]
    fn test_add_flv_operation() {
        let flv_op = Operation::Add(WriteRows {
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
        });

        let expected_result = "{\"Add\":{\"rows\":[{\"cols\":[{\"String\":\"John\"},{\"String\":\"tttt\"},{\"String\":\"m\"},{\"Date\":{\"year\":2000,\"month\":3,\"day\":30}}]}]}}";
        let json_data = serde_json::to_string(&flv_op).unwrap();
        assert_eq!(json_data, expected_result.to_owned());
    }

    #[test]
    fn test_delete_flv_operation() {
        let flv_op = Operation::Delete(DeleteRows {
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
        });

        let expected_result = "{\"Delete\":{\"rows\":[{\"cols\":[{\"String\":\"John\"},{\"String\":\"tttt\"},{\"String\":\"m\"},{\"Date\":{\"year\":2000,\"month\":3,\"day\":30}}]}]}}";
        let json_data = serde_json::to_string(&flv_op).unwrap();
        assert_eq!(json_data, expected_result.to_owned());
    }
}
