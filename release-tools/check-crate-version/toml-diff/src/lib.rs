use std::cmp::Ordering;
use std::mem::discriminant;

use toml::Value as TomlValue;

mod display;
#[cfg(test)]
mod test;

pub struct TomlDiff<'a> {
    pub changes: Vec<TomlChange<'a>>,
}

#[derive(Debug)]
pub enum TomlChange<'a> {
    Added(Vec<&'a str>, &'a TomlValue),
    Deleted(Vec<&'a str>, &'a TomlValue),
}

impl<'a> TomlDiff<'a> {
    /// Return a list of differences between `a` and `b`. A is considered "new" and `b` is
    /// considered "old", so items missing from `a` are considered "deletions", while items
    /// missing from `b` are considered "additions".
    ///
    /// Changes in table keys are always considered either "deletions" or "additions", while
    /// changes in the value of a key are considered "changes".
    pub fn diff(a: &'a TomlValue, b: &'a TomlValue) -> Self {
        if !matches!((a, b), (TomlValue::Table(_), TomlValue::Table(_))) {
            panic!("Expected a table at the top level");
        }
        let mut changes = vec![];
        // Tracks nested Tables and Arrays that are currently being processed.
        // The third element of the tuple is a list of keys that represent the "path" to the
        // current Table or Array.
        let mut stack = vec![(a, b, vec![])];
        while let Some((a, b, key_path)) = stack.pop() {
            match (a, b) {
                (TomlValue::Array(a), TomlValue::Array(b)) => {
                    // Get each value's toml representation and store it alongside
                    let mut a: Vec<_> =
                        a.iter().map(|e| (e, toml::to_string(e).unwrap())).collect();
                    let mut b: Vec<_> =
                        b.iter().map(|e| (e, toml::to_string(e).unwrap())).collect();
                    // Sort the lists by their toml representations
                    a.sort_by(|x, y| x.1.cmp(&y.1));
                    b.sort_by(|x, y| x.1.cmp(&y.1));
                    let mut a = a.into_iter().peekable();
                    let mut b = b.into_iter().peekable();

                    while let (Some((a_elem, a_toml)), Some((b_elem, b_toml))) =
                        (a.peek(), b.peek())
                    {
                        // Toml values are sorted low to high, so if the values are different, that
                        // means that the lesser value is missing from the other array.
                        match a_toml.cmp(b_toml) {
                            Ordering::Less => {
                                // Elements missing from `b` are considered "added" in `a`
                                changes.push(TomlChange::Added(key_path.clone(), a_elem));
                                a.next();
                            }
                            Ordering::Greater => {
                                // Elements missing from `a` are considered "deleted" from `b`
                                changes.push(TomlChange::Deleted(key_path.clone(), b_elem));
                                b.next();
                            }
                            Ordering::Equal => {
                                a.next();
                                b.next();
                            }
                        }
                    }
                    // Anything left over in `a` is an addition (doesn't exist in `b`) and vice versa
                    changes
                        .extend(a.map(|(a_elem, _)| TomlChange::Added(key_path.clone(), a_elem)));
                    changes
                        .extend(b.map(|(b_elem, _)| TomlChange::Deleted(key_path.clone(), b_elem)));
                }
                (TomlValue::Table(a), TomlValue::Table(b)) => {
                    let mut a_pairs: Vec<_> = a.iter().collect();
                    let mut b_pairs: Vec<_> = b.iter().collect();
                    a_pairs.sort_by_key(|e| e.0);
                    b_pairs.sort_by_key(|e| e.0);
                    let mut a_pairs_it = a_pairs.into_iter().peekable();
                    let mut b_pairs_it = b_pairs.into_iter().peekable();

                    while let (Some(a), Some(b)) = (a_pairs_it.peek(), b_pairs_it.peek()) {
                        let (a_key, a_val) = *a;
                        let (b_key, b_val) = *b;
                        // Keys are sorted low to high, so if the keys are different, that means
                        // that the lesser key is missing from the other table.
                        match a_key.cmp(b_key) {
                            Ordering::Less => {
                                // Keys missing from `b` are considered "added" in `a`
                                let mut key_path = key_path.clone();
                                key_path.push(a_key);
                                changes.push(TomlChange::Added(key_path, a_val));
                                a_pairs_it.next();
                                continue;
                            }
                            Ordering::Greater => {
                                // Keys missing from `a` are considered "deleted" from `b`
                                let mut key_path = key_path.clone();
                                key_path.push(b_key);
                                changes.push(TomlChange::Deleted(key_path, b_val));
                                b_pairs_it.next();
                                continue;
                            }
                            Ordering::Equal => {
                                a_pairs_it.next();
                                b_pairs_it.next();

                                // Keys are the same
                                if a_val == b_val {
                                    continue;
                                }
                                // Values are different
                                let mut key_path = key_path.clone();
                                key_path.push(a_key);

                                if discriminant(a_val) != discriminant(b_val) {
                                    // Values have different types
                                    changes.push(TomlChange::Added(key_path.clone(), a_val));
                                    changes.push(TomlChange::Deleted(key_path, b_val));
                                    continue;
                                }
                                if a_val.is_table() || a_val.is_array() {
                                    stack.push((a_val, b_val, key_path));
                                    continue;
                                }
                                changes.push(TomlChange::Added(key_path.clone(), a_val));
                                changes.push(TomlChange::Deleted(key_path, b_val));
                            }
                        }
                    }
                    // Anything left over in `a_pairs_it` is an addition (doesn't exist in `b`) and vice versa
                    changes.extend(a_pairs_it.map(|(k, v)| {
                        let mut key_path = key_path.clone();
                        key_path.push(k);
                        TomlChange::Added(key_path, v)
                    }));
                    changes.extend(b_pairs_it.map(|(k, v)| {
                        let mut key_path = key_path.clone();
                        key_path.push(k);
                        TomlChange::Deleted(key_path, v)
                    }))
                }
                _ => unreachable!("We only ever push `Array`s and `Table`s to `stack`"),
            }
        }
        Self { changes }
    }
}
