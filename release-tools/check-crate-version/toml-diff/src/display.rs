use std::fmt;

use toml::{map::Map, Value as TomlValue};

use crate::{TomlChange, TomlDiff};

impl<'a> fmt::Display for TomlDiff<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for change in &self.changes {
            match change {
                TomlChange::Same => Ok(()),
                TomlChange::Added(key_path, val) => {
                    write!(f, "{}", format_change('+', key_path.clone(), val)?)
                }
                TomlChange::Deleted(key_path, val) => {
                    write!(f, "{}", format_change('-', key_path.clone(), val)?)
                }
            }?;
        }
        Ok(())
    }
}

fn format_change<'a>(
    prefix: char,
    key_path: Vec<&'a str>,
    val: &'a TomlValue,
) -> Result<String, fmt::Error> {
    let s = if key_path.is_empty() {
        toml::to_string(val)
    } else {
        // For each key in key_path, wrap the value in a map
        let mut val = val.clone();
        for &key in key_path.iter().rev() {
            let mut map = Map::new();
            map.insert(key.to_owned(), val.clone());
            val = TomlValue::Table(map);
        }
        toml::to_string(&val)
    }
    .map_err(|_| fmt::Error)?;
    // Prepend the prefix to each line
    Ok(s.lines()
        .map(|line| format!("{prefix} {line}\n"))
        .collect::<Vec<_>>()
        .join(""))
}
