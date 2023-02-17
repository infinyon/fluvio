use super::{TomlChange, TomlDiff};
use std::fs::read;
use toml::Value as TomlValue;

const RED: &str = "\u{1b}[31m";
const GREEN: &str = "\u{1b}[32m";
const RESET: &str = "\u{1b}[0m";

#[test]
fn test_control() {
    let (a, b) = get_toml_values("control_a", "control_b");
    let diff = TomlDiff::diff(&a, &b);
    let changes = diff.changes;
    assert!(changes.is_empty());
}

#[test]
fn test_display_control() {
    let diff = get_diff("control_a", "control_b");
    let expected = "";
    println!("Expected:\n{expected}");
    println!("Actual:\n{diff}");
    assert_eq!(diff, expected);
}

#[test]
fn test_string() {
    let (a, b) = get_toml_values("strings_a", "strings_b");
    let diff = TomlDiff::diff(&a, &b);
    let changes = diff.changes;
    assert_eq!(changes.len(), 4);
    assert!(matches!(
        &changes[0],
        TomlChange::Added(key_path, TomlValue::String(val))
            if key_path[0] == "b" && val == "def"
    ));
    assert!(matches!(
        &changes[1],
        TomlChange::Deleted(key_path, TomlValue::String(val))
            if key_path[0] == "c" && val == "ghi"
    ));
    assert!(matches!(
        &changes[2],
        TomlChange::Added(key_path, TomlValue::String(val))
            if key_path[0] == "e" && val == "mno"
    ));
    assert!(matches!(
        &changes[3],
        TomlChange::Added(key_path, TomlValue::String(val))
            if key_path[0] == "f" && val == "pqr"
    ));
}

#[test]
fn test_display_string() {
    let diff = get_diff("strings_a", "strings_b");
    let expected = format!(
        "\
{GREEN}+ b = \"def\"{RESET}
{RED}- c = \"ghi\"{RESET}
{GREEN}+ e = \"mno\"{RESET}
{GREEN}+ f = \"pqr\"{RESET}
"
    );
    println!("Expected:\n{expected}");
    println!("Actual:\n{diff}");
    assert_eq!(diff, expected);
}

#[test]
fn test_array() {
    let (a, b) = get_toml_values("arrays_a", "arrays_b");
    let diff = TomlDiff::diff(&a, &b);
    let changes = diff.changes;
    assert_eq!(changes.len(), 4);
    assert!(matches!(
        &changes[0],
        TomlChange::Added(key_path, TomlValue::Array(val))
            if key_path[0] == "a"
                && matches!(val[0], TomlValue::Integer(1))
                && matches!(val[1], TomlValue::Integer(2))
                && matches!(val[2], TomlValue::Integer(3))
    ));
    assert!(matches!(
        &changes[1],
        TomlChange::Deleted(key_path, TomlValue::Array(val))
            if key_path[0] == "c"
                && matches!(val[0], TomlValue::Integer(3))
                && matches!(val[1], TomlValue::Integer(4))
                && matches!(val[2], TomlValue::Integer(5))
    ));
    assert!(matches!(
        &changes[2],
        TomlChange::Deleted(key_path, TomlValue::Array(val))
            if key_path[0] == "e"
                && matches!(val[0], TomlValue::Integer(5))
                && matches!(val[1], TomlValue::Integer(6))
                && matches!(val[2], TomlValue::Integer(7))
    ));
    assert!(matches!(
        &changes[3],
        TomlChange::Deleted(key_path, TomlValue::Array(val))
            if key_path[0] == "f"
                && matches!(val[0], TomlValue::Integer(6))
                && matches!(val[1], TomlValue::Integer(7))
                && matches!(val[2], TomlValue::Integer(8))
    ));
}

#[test]
fn test_display_array() {
    let diff = get_diff("arrays_a", "arrays_b");
    let expected = format!(
        "\
{GREEN}+ a = [1, 2, 3]{RESET}
{RED}- c = [3, 4, 5]{RESET}
{RED}- e = [5, 6, 7]{RESET}
{RED}- f = [6, 7, 8]{RESET}
"
    );
    println!("Expected:\n{expected}");
    println!("Actual:\n{diff}");
    assert_eq!(diff, expected);
}

#[test]
fn test_table() {
    let (a, b) = get_toml_values("tables_a", "tables_b");
    let diff = TomlDiff::diff(&a, &b);
    let changes = diff.changes;
    assert_eq!(changes.len(), 2);
    assert!(matches!(
        &changes[0],
        TomlChange::Added(key_path, TomlValue::Table(table))
            if key_path[0] == "b"
                && matches!(&table["c"], TomlValue::String(val) if val == "ghi")
                && matches!(&table["d"], TomlValue::String(val) if val == "jkl")
    ));
    assert!(matches!(
        &changes[1],
        TomlChange::Deleted(key_path, TomlValue::Table(table))
            if key_path[0] == "c"
                && matches!(&table["e"], TomlValue::String(val) if val == "nmo")
                && matches!(&table["f"], TomlValue::String(val) if val == "pqr")
    ));
}

#[test]
fn test_display_table() {
    let diff = get_diff("tables_a", "tables_b");
    let expected = format!(
        "\
{GREEN}+ [b]{RESET}
{GREEN}+ c = \"ghi\"{RESET}
{GREEN}+ d = \"jkl\"{RESET}
{RED}- [c]{RESET}
{RED}- e = \"nmo\"{RESET}
{RED}- f = \"pqr\"{RESET}
"
    );
    println!("Expected:\n{expected}");
    println!("Actual:\n{diff}");
    assert_eq!(diff, expected);
}

#[test]
fn test_nested_table() {
    let (a, b) = get_toml_values("nested_tables_a", "nested_tables_b");
    let diff = TomlDiff::diff(&a, &b);
    let changes = diff.changes;
    assert_eq!(changes.len(), 2);
    assert!(matches!(
        &changes[0],
        TomlChange::Added(key_path, TomlValue::Table(table))
            if key_path[0] == "outer"
                && key_path[1] == "inner_b"
                && matches!(&table["b"], TomlValue::Integer(2))
    ));
    assert!(matches!(
        &changes[1],
        TomlChange::Deleted(key_path, TomlValue::Table(table))
            if key_path[0] == "outer"
                && key_path[1] == "inner_c"
                && matches!( &table["c"], TomlValue::Integer(3))
    ));
}

#[test]
fn test_display_nested_table() {
    let diff = get_diff("nested_tables_a", "nested_tables_b");
    let expected = format!(
        "\
{GREEN}+ [outer.inner_b]{RESET}
{GREEN}+ b = 2{RESET}
{RED}- [outer.inner_c]{RESET}
{RED}- c = 3{RESET}
"
    );
    println!("Expected:\n{expected}");
    println!("Actual:\n{diff}");
    assert_eq!(diff, expected);
}

#[test]
fn test_array_reorder() {
    let (a, b) = get_toml_values("array_reorder_a", "array_reorder_b");
    let diff = TomlDiff::diff(&a, &b);
    let changes = diff.changes;
    assert!(changes.is_empty());
}

#[test]
fn test_display_array_reorder() {
    let diff = get_diff("array_reorder_a", "array_reorder_b");
    let expected = "";
    println!("Expected:\n{expected}");
    println!("Actual:\n{diff}");
    assert_eq!(diff, expected);
}

#[test]
fn test_display_array_delete() {
    let diff = get_diff("array_delete_a", "array_delete_b");
    let expected = format!("{RED}- array = \"element_b\"{RESET}\n");
    println!("Expected:\n{expected}");
    println!("Actual:\n{diff}");
    assert_eq!(diff, expected);
}

#[test]
fn test_display_array_add() {
    let diff = get_diff("array_add_a", "array_add_b");
    let expected = format!("{GREEN}+ array = \"element_b\"{RESET}\n");
    println!("Expected:\n{expected}");
    println!("Actual:\n{diff}");
    assert_eq!(diff, expected);
}

#[test]
fn test_display_array_delete_one() {
    let diff = get_diff("array_delete_one_a", "array_delete_one_b");
    let expected = format!("{RED}- array = \"element\"{RESET}\n");
    println!("Expected:\n{expected}");
    println!("Actual:\n{diff}");
    assert_eq!(diff, expected);
}

fn get_toml_values(a: &str, b: &str) -> (TomlValue, TomlValue) {
    let a = read(format!("./test_data/{a}.toml")).unwrap();
    let b = read(format!("./test_data/{b}.toml")).unwrap();
    let a = String::from_utf8_lossy(&a);
    let b = String::from_utf8_lossy(&b);
    let a: TomlValue = toml::from_str(&a).unwrap();
    let b: TomlValue = toml::from_str(&b).unwrap();
    (a, b)
}

fn get_diff(a: &str, b: &str) -> String {
    let (a, b) = get_toml_values(a, b);
    let diff = TomlDiff::diff(&a, &b);
    diff.to_string()
}
