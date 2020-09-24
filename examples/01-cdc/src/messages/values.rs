use bigdecimal::BigDecimal;
use chrono::prelude::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{Duration, UNIX_EPOCH};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum Value {
    UnsignedInteger(u8),
    SignedInteger(i64),
    Float(f32),
    Double(f64),
    String(String),
    Enum(i16),
    Blob(Vec<u8>),
    Year(u32),
    Date {
        year: u32,
        month: u32,
        day: u32,
    },
    Time {
        hours: u32,
        minutes: u32,
        seconds: u32,
        subseconds: u32,
    },
    DateTime {
        year: u32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        subsecond: u32,
    },
    Json(serde_json::Value),
    Decimal(BigDecimal),
    Timestamp {
        unix_time: i32,
        subsecond: u32,
    },
    Null,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::UnsignedInteger(val) => write!(f, "{}", val),
            Value::SignedInteger(val) => write!(f, "{}", val),
            Value::Float(val) => write!(f, "{}", val),
            Value::Double(val) => write!(f, "{}", val),
            Value::String(val) => write!(f, "\"{}\"", val),
            Value::Enum(val) => write!(f, "{}", val),
            Value::Blob(bytes) => {
                // TODO: add binary support
                let val = String::from_utf8(bytes.clone()).expect("error vec => string");
                write!(f, "{}", val)
            }
            Value::Year(val) => write!(f, "\"{}\"", val),
            Value::Date { year, month, day } => write!(f, "\"{}-{}-{}\"", year, month, day),
            Value::Time {
                hours,
                minutes,
                seconds,
                subseconds,
            } => write!(f, "\"{}:{}:{}.{}\"", hours, minutes, seconds, subseconds),
            Value::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                subsecond,
            } => write!(
                f,
                "\"{}-{}-{} {}:{}:{}.{}\"",
                year, month, day, hour, minute, second, subsecond
            ),
            Value::Json(val) => write!(f, "{}", val),
            Value::Decimal(val) => write!(f, "{}", val),
            Value::Timestamp { unix_time, .. } => {
                let d = UNIX_EPOCH + Duration::from_secs(*unix_time as u64);
                let datetime = DateTime::<Utc>::from(d);
                let value = datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string();
                write!(f, "\"{}\"", value)
            }
            Value::Null => write!(f, "Null"),
        }
    }
}

// Creates a new SystemTime from the specified number of whole seconds

#[cfg(test)]
mod test {
    use super::Value;
    use bigdecimal::BigDecimal;
    use serde_json::value::Value as JsonValue;

    #[test]
    fn test_string_flv_values() {
        let val = Value::String("Puffball".to_owned());

        let json_data = serde_json::to_string(&val).unwrap();
        assert_eq!(json_data, "{\"String\":\"Puffball\"}".to_owned());

        let val_res: Result<Value, _> = serde_json::from_str(&json_data);
        assert!(val_res.is_ok());
        assert_eq!(val_res.unwrap(), val);
    }

    #[test]
    fn test_date_flv_value() {
        let val = Value::Date {
            year: 2000,
            month: 3,
            day: 30,
        };

        let json_data = serde_json::to_string(&val).unwrap();
        assert_eq!(
            json_data,
            "{\"Date\":{\"year\":2000,\"month\":3,\"day\":30}}".to_owned()
        );

        let val_res: Result<Value, _> = serde_json::from_str(&json_data);
        assert!(val_res.is_ok());
        assert_eq!(val_res.unwrap(), val);
    }

    #[test]
    fn test_display_values() {
        assert_eq!(format!("{}", Value::UnsignedInteger(10)), "10");
        assert_eq!(format!("{}", Value::SignedInteger(-10)), "-10");

        assert_eq!(
            format!("{}", Value::Blob(String::from("hello").into_bytes())),
            "hello".to_owned()
        );

        assert_eq!(format!("{}", Value::Year(1999)), "\"1999\"");

        assert_eq!(
            format!(
                "{}",
                Value::Date {
                    year: 2000,
                    month: 3,
                    day: 30,
                }
            ),
            "\"2000-3-30\""
        );

        assert_eq!(
            format!(
                "{}",
                Value::Time {
                    hours: 21,
                    minutes: 31,
                    seconds: 30,
                    subseconds: 1000
                }
            ),
            "\"21:31:30.1000\""
        );

        assert_eq!(
            format!(
                "{}",
                Value::DateTime {
                    year: 2000,
                    month: 3,
                    day: 30,
                    hour: 21,
                    minute: 31,
                    second: 30,
                    subsecond: 1000
                }
            ),
            "\"2000-3-30 21:31:30.1000\""
        );

        assert_eq!(
            format!("{}", Value::Json(JsonValue::String("10".to_string()))),
            "\"10\"".to_owned()
        );

        assert_eq!(
            format!("{}", "5.54321".parse::<BigDecimal>().unwrap()),
            "5.54321".to_owned()
        );

        assert_eq!(
            format!(
                "{}",
                Value::Timestamp {
                    unix_time: 1524885322,
                    subsecond: 0
                }
            ),
            "\"2018-04-28 03:15:22.000000000\"".to_owned()
        );
    }
}
