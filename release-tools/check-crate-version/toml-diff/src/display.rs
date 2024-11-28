use std::fmt;

use toml::{map::Map, Value as TomlValue};

use crate::{TomlChange, TomlDiff};

impl fmt::Display for TomlDiff<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for change in &self.changes {
            match change {
                TomlChange::Added(key_path, val) => {
                    writeln!(
                        f,
                        "{}",
                        format_change(ChangeKind::Added, key_path.clone(), val)?
                    )
                }
                TomlChange::Deleted(key_path, val) => {
                    writeln!(
                        f,
                        "{}",
                        format_change(ChangeKind::Deleted, key_path.clone(), val)?
                    )
                }
            }?;
        }
        Ok(())
    }
}

enum ChangeKind {
    Added,
    Deleted,
}

const RED: &str = "\u{1b}[31m";
const GREEN: &str = "\u{1b}[32m";
const RESET: &str = "\u{1b}[0m";

fn format_change<'a>(
    change_kind: ChangeKind,
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
        .map(|line| match change_kind {
            ChangeKind::Added => format!("{GREEN}+ {line}{RESET}"),
            ChangeKind::Deleted => format!("{RED}- {line}{RESET}"),
        })
        .collect::<Vec<_>>()
        .join("\n"))
}
