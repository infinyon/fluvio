//!
//! # String helpers
//!

/// Convert string in UpperCammelCase to senteces
pub fn upper_cammel_case_to_sentence(src: String, remove_first: bool) -> String {
    // convert "ThisIsATest" to "this is a test"
    let mut letters: Vec<_> = vec![];
    for c in src.chars() {
        if c.is_uppercase() {
            if !letters.is_empty() {
                letters.push(' ');
            }
            if let Some(lowercase) = c.to_lowercase().to_string().chars().next() {
                letters.push(lowercase);
            }
        } else {
            letters.push(c);
        }
    }
    let sentence: String = letters.into_iter().collect();

    // remove first word "this is a test" to "is a test"
    if remove_first {
        let mut words: Vec<_> = sentence.split_whitespace().collect();
        words.drain(0..1);
        words.join(" ")
    } else {
        sentence
    }
}

//
// Unit Tests
//

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn test_upper_cammel_case_to_sentence() {
        assert_eq!(
            upper_cammel_case_to_sentence("ThisIsATest".to_owned(), false),
            "this is a test"
        );

        assert_eq!(
            upper_cammel_case_to_sentence("ThisIsATest".to_owned(), true),
            "is a test"
        );

        assert_eq!(
            upper_cammel_case_to_sentence("TopicAlreadyExists".to_owned(), true),
            "already exists"
        );

        assert_eq!(
            upper_cammel_case_to_sentence("UnknownTopicOrPartition".to_owned(), false),
            "unknown topic or partition"
        );

    }
}