pub(crate) fn pointer<'a>(
    value: &'a serde_json::Value,
    pointer: &str,
) -> Option<&'a serde_json::Value> {
    if pointer.eq("$") || pointer.is_empty() {
        value.pointer("")
    } else {
        let pointer = pointer.strip_prefix("$.").unwrap_or(pointer);
        let mut normalized = pointer.replace('.', "/");
        if !normalized.starts_with('/') {
            normalized.insert(0, '/');
        }
        value.pointer(normalized.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_root() {
        //given
        let input = json!({
           "key1": "value1"
        });

        //when
        let option1 = pointer(&input, "$");
        let option2 = pointer(&input, "");

        //then
        assert_eq!(option1, Some(&input));
        assert_eq!(option2, Some(&input));
    }

    #[test]
    fn test_field() {
        //given
        let input = json!({
           "key1": "value1"
        });

        //when
        let option1 = pointer(&input, "key1");
        let option2 = pointer(&input, ".key1");
        let option3 = pointer(&input, "$.key1");

        //then
        assert!(option1.is_some());
        let value = serde_json::Value::String("value1".to_string());
        assert_eq!(option1, Some(&value));
        assert_eq!(option2, Some(&value));
        assert_eq!(option3, Some(&value));
    }

    #[test]
    fn test_nested_field() {
        //given
        let input = json!({
           "key1": {"key2": "value2"}
        });

        //when
        let option1 = pointer(&input, "key1.key2");
        let option2 = pointer(&input, ".key1.key2");
        let option3 = pointer(&input, "$.key1.key2");

        //then
        assert!(option1.is_some());
        let value = serde_json::Value::String("value2".to_string());
        assert_eq!(option1, Some(&value));
        assert_eq!(option2, Some(&value));
        assert_eq!(option3, Some(&value));
    }

    #[test]
    fn test_nested_array() {
        //given
        let input = json!({
           "key1": {"key2": ["one", "two", "three"]}
        });

        //when
        let option1 = pointer(&input, "key1.key2.0");
        let option2 = pointer(&input, ".key1.key2.1");
        let option3 = pointer(&input, "$.key1.key2.2");

        //then
        assert_eq!(option1, Some(&json!("one")));
        assert_eq!(option2, Some(&json!("two")));
        assert_eq!(option3, Some(&json!("three")));
    }
}
