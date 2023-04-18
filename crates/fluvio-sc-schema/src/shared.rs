const K8S_MAX_RESOURCE_NAME_LEN: usize = 63;

pub fn is_valid_k8s_resource_name(name: &str) -> bool {
    if name.len() > K8S_MAX_RESOURCE_NAME_LEN {
        return false;
    }

    name.chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
        && !name.ends_with('-')
        && !name.starts_with('-')
}

#[cfg(test)]
mod test {
    use super::is_valid_k8s_resource_name;

    #[test]
    fn validates_name_length() {
        let name = "this-is-a-very-long-long-long-long-long-long-name-and-its-not-valid";

        assert!(!is_valid_k8s_resource_name(name));
    }

    #[test]
    fn validates_no_spaces_allowed() {
        let name = "Hello World";

        assert!(!is_valid_k8s_resource_name(name));
    }

    #[test]
    fn validates_no_special_chars_allowed() {
        let name = "!@#$%^&*()👻";

        assert!(!is_valid_k8s_resource_name(name));
    }

    #[test]
    fn allows_valid_names() {
        let names = vec![
            "prices-list-scrapper",
            "final-countdown-actual-countdown-timer",
            "luke-i-am-your-father",
            "im-not-looking-for-funny-names-in-the-internet",
            "use-fluvio-exclamation-sign",
            "testing-1234",
        ];

        for name in names {
            assert!(is_valid_k8s_resource_name(name));
        }
    }

    #[test]
    fn reject_topics_with_spaces() {
        assert!(!is_valid_k8s_resource_name("hello world"));
    }

    #[test]
    fn reject_topics_with_uppercase() {
        assert!(!is_valid_k8s_resource_name("helloWorld"));
    }

    #[test]
    fn reject_topics_with_underscore() {
        assert!(!is_valid_k8s_resource_name("hello_world"));
    }

    #[test]
    fn valid_topic() {
        assert!(is_valid_k8s_resource_name("hello-world"));
    }
    #[test]
    fn reject_topics_that_start_with_hyphen() {
        assert!(!is_valid_k8s_resource_name("-helloworld"));
    }
}
