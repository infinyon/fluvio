//!
//! # Counters
//!
//! Counters object definition and functionality
//!
use std::cmp;
use std::collections::BTreeMap;

#[derive(Debug, PartialEq)]
pub struct Counters<T> {
    pub list: BTreeMap<T, Counter>,
}

#[derive(Debug, PartialEq)]
pub struct Counter {
    label: &'static str,
    internal: bool,
    value: u32,
}

impl<T> ::std::default::Default for Counters<T>
where
    T: Ord,
{
    fn default() -> Self {
        Self {
            list: BTreeMap::new(),
        }
    }
}

impl<T> Counters<T>
where
    T: Ord,
{
    /// build counter from array of tuples
    pub fn new(items: Vec<(T, &'static str, bool)>) -> Self {
        let mut counters = Counters::default();
        for (id, label, internal) in items {
            counters.list.insert(
                id,
                Counter {
                    label,
                    internal,
                    value: 0,
                },
            );
        }
        counters
    }

    /// increment counter
    pub fn inc_counter(&mut self, id: T) {
        if let Some(counter) = self.list.get_mut(&id) {
            counter.value += 1;
        }
    }

    /// increment counter
    pub fn set_counter(&mut self, id: T, val: u32) {
        if let Some(counter) = self.list.get_mut(&id) {
            counter.value = val;
        }
    }

    /// reset counters
    pub fn reset(&mut self) {
        for counter in self.list.values_mut() {
            counter.value = 0;
        }
    }

    /// counter heders in string format (center justified, min 10 spaces)
    pub fn header_fmt(&self) -> String {
        let mut res = String::new();

        // accumulate labels
        for counter in self.list.values() {
            res.push_str(&format!("{:^10}", counter.label));
            res.push_str("  ");
        }

        // remove last 2 spaces
        if res.len() > 2 {
            res.truncate(res.len() - 2);
        }

        res
    }

    /// format values in string format (center justified to column header - min 10 spaces)
    pub fn values_fmt(&self) -> String {
        let mut res = String::new();

        // accumulate labels
        for counter in self.list.values() {
            let value_str = counter.value.to_string();
            let space_width = cmp::max(counter.label.len(), 10);
            let value_width = value_str.len();

            // center justify... need to compute our own adding (as Rust formatter requires literal)
            let (pad_left, pad_right) = if value_width > space_width {
                (0, 0)
            } else {
                let pad_left = (space_width - value_width) / 2;
                let pad_right = space_width - pad_left - value_width;
                (pad_left, pad_right)
            };

            res.push_str(&(0..pad_left).map(|_| " ").collect::<String>());
            res.push_str(&value_str);
            res.push_str(&(0..pad_right).map(|_| " ").collect::<String>());
            res.push_str("  ");
        }

        // remove last 2 spaces
        if res.len() > 2 {
            res.truncate(res.len() - 2);
        }

        res
    }
}

// -----------------------------------
// Unit Tests
// -----------------------------------

#[cfg(test)]
pub mod test {
    use super::*;

    #[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
    enum TestCntr {
        Ok = 0,
        Failed = 1,
        Retry = 2,
        Shutdown = 3,
        InternalErr = 4,
    }

    fn generate_counters() -> Vec<(TestCntr, &'static str, bool)> {
        vec![
            (TestCntr::Ok, "CONN-OK", false),
            (TestCntr::Failed, "CONN-FAILED", false),
            (TestCntr::Retry, "CONN-RETRY", false),
            (TestCntr::Shutdown, "CONN-SHUTDOWN", false),
            (TestCntr::InternalErr, "INTERNAL-ERR", true),
        ]
    }

    #[test]
    fn test_counters_all() {
        let mut counters = Counters::new(generate_counters());

        // test generation
        assert_eq!(counters.list.len(), 5);

        // test header formatter
        let header = counters.header_fmt();
        let expected_header =
            " CONN-OK    CONN-FAILED  CONN-RETRY  CONN-SHUTDOWN  INTERNAL-ERR".to_owned();
        assert_eq!(header, expected_header);

        // test increment & value formatter
        counters.set_counter(TestCntr::Ok, 4294967290);
        counters.inc_counter(TestCntr::Failed);
        counters.set_counter(TestCntr::Retry, 4199999999);
        counters.inc_counter(TestCntr::Shutdown);
        counters.inc_counter(TestCntr::Shutdown);
        let values = counters.values_fmt();
        let expected_values =
            "4294967290       1       4199999999        2             0      ".to_owned();
        assert_eq!(values, expected_values);

        // test equal
        let mut expected_counters = Counters::new(generate_counters());
        expected_counters.set_counter(TestCntr::Ok, 4294967290);
        expected_counters.set_counter(TestCntr::Failed, 1);
        expected_counters.set_counter(TestCntr::Retry, 4199999999);
        expected_counters.set_counter(TestCntr::Shutdown, 2);
        assert_eq!(counters, expected_counters);

        // test reset
        counters.reset();
        assert_eq!(counters, Counters::new(generate_counters()));
        assert_eq!(counters.list.len(), 5);
    }

}
