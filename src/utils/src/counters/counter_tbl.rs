//!
//! # Counter Table
//!
//! Stores counters in table costruct. Each table has a header and rows with counters for each column.
//!
use std::cmp;
use std::collections::BTreeMap;
use std::sync::RwLock;

#[derive(Debug)]
pub struct CounterTable<T, C> {
    pub columns: BTreeMap<C, Column>,
    pub rows: RwLock<BTreeMap<T, BTreeMap<C, u32>>>,
}

#[derive(Debug, PartialEq)]
pub struct Column {
    label: &'static str,
    internal: bool,
}

impl<T, C> ::std::default::Default for CounterTable<T, C>
where
    C: Ord,
    T: Ord,
{
    fn default() -> Self {
        Self {
            columns: BTreeMap::new(),
            rows: RwLock::new(BTreeMap::new()),
        }
    }
}

impl<T, C> ::std::cmp::PartialEq for CounterTable<T, C>
where
    C: PartialEq,
    T: PartialEq,
{
    fn eq(&self, other: &CounterTable<T, C>) -> bool {
        // compare columns
        if self.columns != other.columns {
            return false;
        }

        // compare counters
        let local_rows = self.rows.read().unwrap();
        let other_rows = other.rows.read().unwrap();
        if *local_rows != *other_rows {
            return false;
        }
        true
    }
}

impl<T, C> CounterTable<T, C>
where
    C: Ord + Clone,
    T: Ord,
{
    /// builder pattern to add columns
    pub fn with_columns(mut self, columns: Vec<(C, &'static str, bool)>) -> Self {
        for (column_id, label, internal) in columns {
            self.columns.insert(column_id, Column { label, internal });
        }
        self
    }

    /// create a row and add counter columns
    pub fn add_row(&self, row_id: T) {
        // add one counter per column
        let mut column_counters = BTreeMap::new();
        for column_id in self.columns.keys() {
            column_counters.insert(column_id.clone(), 0);
        }

        // add counters to row
        self.rows.write().unwrap().insert(row_id, column_counters);
    }

    /// remove counter row
    pub fn remove_row(&self, row_id: &T) {
        self.rows.write().unwrap().remove(row_id);
    }

    /// number of rows
    pub fn row_count(&self) -> usize {
        self.rows.read().unwrap().len()
    }

    /// increment counter
    pub fn inc_counter(&self, row_id: &T, column_id: C) {
        if let Some(row) = self.rows.write().unwrap().get_mut(row_id) {
            if let Some(counter) = row.get_mut(&column_id) {
                *counter += 1;
            }
        }
    }

    /// set counter
    pub fn set_counter(&self, row_id: &T, column_id: C, val: u32) {
        if let Some(row) = self.rows.write().unwrap().get_mut(row_id) {
            if let Some(counter) = row.get_mut(&column_id) {
                *counter = val;
            }
        }
    }

    /// reset all counters
    pub fn reset_counters(&self) {
        for row in self.rows.write().unwrap().values_mut() {
            for counter in row.values_mut() {
                *counter = 0;
            }
        }
    }

    /// column headers in string format (center justified, min 10 spaces)
    pub fn header_fmt(&self) -> String {
        let mut res = String::new();

        // accumulate labels
        for column in self.columns.values() {
            res.push_str(&format!("{:^10}", column.label));
            res.push_str("  ");
        }

        // remove last 2 spaces
        if res.len() > 2 {
            res.truncate(res.len() - 2);
        }

        res
    }

    /// format values in string format (center justified to column header - min 10 spaces)
    pub fn values_fmt(&self, row_id: T) -> String {
        let mut res = String::new();
        if let Some(row) = self.rows.write().unwrap().get_mut(&row_id) {
            // accumulate labels
            for (column_id, counter) in row {
                let value_str = counter.to_string();
                let column_label = if let Some(column) = self.columns.get(column_id) {
                    <&str>::clone(&column.label)
                } else {
                    ""
                };
                let space_width = cmp::max(column_label.len(), 10);
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

    #[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
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
    fn test_counter_table_all() {
        let counter_tbl = CounterTable::default().with_columns(generate_counters());

        // test generation
        assert_eq!(counter_tbl.columns.len(), 5);

        // test add rows
        let (row0, row1): (i32, i32) = (0, 1);
        counter_tbl.add_row(row0);
        counter_tbl.add_row(row1);
        assert_eq!(counter_tbl.row_count(), 2);

        // test header formatter
        let header = counter_tbl.header_fmt();
        let expected_header =
            " CONN-OK    CONN-FAILED  CONN-RETRY  CONN-SHUTDOWN  INTERNAL-ERR".to_owned();
        assert_eq!(header, expected_header);

        // test increment & value formatter
        counter_tbl.set_counter(&row0, TestCntr::Ok, 4294967290);
        counter_tbl.inc_counter(&row0, TestCntr::Failed);
        counter_tbl.set_counter(&row0, TestCntr::Retry, 4199999999);
        let values_r1 = counter_tbl.values_fmt(row0);
        let expected_values_r1 =
            "4294967290       1       4199999999        0             0      ".to_owned();
        assert_eq!(values_r1, expected_values_r1);

        counter_tbl.inc_counter(&row1, TestCntr::Shutdown);
        counter_tbl.inc_counter(&row1, TestCntr::Shutdown);
        let values_r2 = counter_tbl.values_fmt(row1);
        let expected_values_r2 =
            "    0            0           0             2             0      ".to_owned();
        assert_eq!(values_r2, expected_values_r2);

        // test equality
        let expected_counter_tbl = CounterTable::default().with_columns(generate_counters());
        expected_counter_tbl.add_row(row0);
        expected_counter_tbl.add_row(row1);
        expected_counter_tbl.set_counter(&row0, TestCntr::Ok, 4294967290);
        expected_counter_tbl.set_counter(&row0, TestCntr::Failed, 1);
        expected_counter_tbl.set_counter(&row0, TestCntr::Retry, 4199999999);
        expected_counter_tbl.set_counter(&row1, TestCntr::Shutdown, 2);
        assert_eq!(counter_tbl, expected_counter_tbl);

        // test reset
        counter_tbl.reset_counters();
        let expected_reset_tbl = CounterTable::default().with_columns(generate_counters());
        expected_reset_tbl.add_row(row0);
        expected_reset_tbl.add_row(row1);
        assert_eq!(counter_tbl, expected_reset_tbl);
        assert_eq!(counter_tbl.row_count(), 2);

        // teest remoev row
        counter_tbl.remove_row(&row0);
        let expected_one_row_tbl = CounterTable::default().with_columns(generate_counters());
        expected_one_row_tbl.add_row(row1);
        assert_eq!(counter_tbl, expected_one_row_tbl);
        assert_eq!(counter_tbl.row_count(), 1);
    }
}
