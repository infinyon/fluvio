mod diff;
mod se;

use serde_json::Map;
use serde_json::Value;
use std::collections::HashMap;

use crate::Changes;
use crate::Diff;
use crate::DiffError;

type SerdeObj = Map<String, Value>;
pub type JsonDiff = Diff<Value, PatchObject>;

#[derive(Debug)]
pub struct PatchObject(HashMap<String, JsonDiff>);

impl PatchObject {
    // diff { "a": 1,"b": 2}, { "a": 3, "b": 2} => { "a": 1}
    fn diff(old: &SerdeObj, new: &SerdeObj) -> Result<Self, DiffError> {
        let mut map: HashMap<String, JsonDiff> = HashMap::new();

        for (key, new_val) in new.iter() {
            match old.get(key) {
                Some(old_val) => {
                    if old_val != new_val {
                        let diff_value = old_val.diff(new_val)?;
                        map.insert(key.clone(), diff_value);
                    }
                }
                _ => {
                    map.insert(key.clone(), Diff::Replace(new_val.clone())); // just replace with new if key doesn't match
                }
            }
        }

        Ok(PatchObject(map))
    }

    fn get_inner_ref(&self) -> &HashMap<String, JsonDiff> {
        &self.0
    }
}
