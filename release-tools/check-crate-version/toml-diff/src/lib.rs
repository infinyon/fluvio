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
    Same,
    Added(Vec<&'a str>, &'a TomlValue),
    Deleted(Vec<&'a str>, &'a TomlValue),
}

impl<'a> TomlDiff<'a> {
    /// Return a list of differences between `a` and `b`. A is considered "new" and `b` is
    /// considered "old", so items missing from `a` are considdered "deletions", while items
    /// missing from `b` are considered "additions".
    ///
    /// Changes in table keys are always considdered either "deletions" or "additions", while
    /// changes in the value of a key are considdered "changes".
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
                    let mut a_it = a.iter();
                    let mut b_it = b.iter();

                    // TODO: Ideally we would sort elements first, then track additions and
                    // deletions as we do for keys in Tables, but TomlValue does not implement Ord,
                    // so we can't sort. We could get around this by implementing Ord for
                    // TomlValue.
                    for (a_elem, b_elem) in a_it.by_ref().zip(b_it.by_ref()) {
                        if a_elem == b_elem {
                            // No change in this array element
                            continue;
                        }
                        if discriminant(a_elem) != discriminant(b_elem) {
                            // Elements have different types
                            changes.push(TomlChange::Added(key_path.clone(), a_elem));
                            changes.push(TomlChange::Deleted(key_path.clone(), b_elem));
                            continue;
                        }
                        if a_elem.is_table() || a_elem.is_array() {
                            stack.push((a_elem, b_elem, key_path.clone()));
                        } else {
                            changes.push(TomlChange::Added(key_path.clone(), a_elem));
                            changes.push(TomlChange::Deleted(key_path.clone(), b_elem));
                        }
                    }
                    // Anything left in `a_it` is an addition (doesn't exist in `b`), and vice versa
                    changes.extend(a_it.map(|e| TomlChange::Added(key_path.clone(), e)));
                    changes.extend(b_it.map(|e| TomlChange::Deleted(key_path.clone(), e)));
                }
                (TomlValue::Table(a), TomlValue::Table(b)) => {
                    let mut a_pairs: Vec<_> = a.iter().collect();
                    let mut b_pairs: Vec<_> = b.iter().collect();
                    a_pairs.sort_by_key(|e| e.0);
                    b_pairs.sort_by_key(|e| e.0);
                    let mut a_pairs_it = a_pairs.into_iter().peekable();
                    let mut b_pairs_it = b_pairs.into_iter().peekable();

                    while let (Some((&ref a_key, &ref a_val)), Some((&ref b_key, &ref b_val))) =
                        (a_pairs_it.peek(), b_pairs_it.peek())
                    {
                        // Keys are sorted low to high, so if the keys are different, that means
                        // that the lesser key is missing from the other table.
                        match a_key.cmp(b_key) {
                            Ordering::Less => {
                                // Keys missing from `b` are considdered "added" in `a`
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
                            }
                        }
                        // Keys are the same
                        if a_val == b_val {
                            continue;
                        }
                        let mut key_path = key_path.clone();
                        key_path.push(a_key);

                        // Values are different
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
                    // Anything left over in `a_pairs_it` is an addition (doesn't exist in `b`) and vice versa
                    changes.extend(a_pairs_it.map(|(k, v)| {
                        let mut key_path = key_path.clone();
                        key_path.push(k);
                        let change = TomlChange::Added(key_path, v);
                        change
                    }));
                    changes.extend(b_pairs_it.map(|(k, v)| {
                        let mut key_path = key_path.clone();
                        key_path.push(k);
                        let change = TomlChange::Deleted(key_path, v);
                        change
                    }))
                }
                _ => unreachable!("We only ever push `Array`s and `Table`s to `stack`"),
            }
        }
        Self { changes }
    }
}
