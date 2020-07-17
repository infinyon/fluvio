pub trait KeyFilter<V: ?Sized> {

    fn filter(&self,value: &V) -> bool;
}


impl KeyFilter<str> for str {

    fn filter(&self, value: &str) -> bool {
        value.contains(self)
    }

}

impl KeyFilter<str> for Vec<String> {

    fn filter(&self, value: &str) -> bool {
        if self.len() == 0 {
            return true;
        }
        self.iter()
            .filter(|key| key.filter(value))
            .count() > 0
    }
}

#[cfg(test)]
mod tests {

    use super::KeyFilter;

    #[test]
    fn test_str_filter() {

        let value = "quick brown";
        assert!("quick".filter(value));
    }

    #[test]
    fn test_str_list_filter() {

        let value = "quick brown";
        assert!(vec![].filter(value));
        assert!(vec!["quick".to_owned()].filter(value));
    }

}