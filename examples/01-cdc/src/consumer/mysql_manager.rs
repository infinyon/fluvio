//! MYSQL Manager
//!
//! Responsible for:
//!  - mysql server connection
//!  - converting fluvio messages to mysql query commands
//!
use http::Uri;
use mysql::prelude::*;
use mysql::{Conn, OptsBuilder};
use std::io::{Error, ErrorKind};

use crate::consumer::profile::{Profile, Filters};
use crate::messages::{DeleteRows, UpdateRows, WriteRows};
use crate::messages::{FluvioMessage, Operation, Value};

pub struct MysqlManager {
    conn: Conn,
    db_name: Option<String>,
    filters: Option<Filters>,
}

#[derive(Debug, PartialEq)]
pub struct UriProps {
    db_name: Option<String>,
    table_name: Option<String>,
}

impl MysqlManager {
    /// Use settings in the profile to connect to database
    ///  - db_name is left empty, as it is not know at this time.
    pub fn connect(profile: &Profile) -> Result<Self, Error> {
        let opts = OptsBuilder::new()
            .ip_or_hostname(profile.ip_or_host())
            .tcp_port(profile.port())
            .user(profile.user())
            .pass(profile.password());
        let conn = Conn::new(opts)
            .map_err(|err| Error::new(ErrorKind::ConnectionRefused, format!("{}", err)))?;

        Ok(Self {
            conn,
            db_name: None,
            filters: profile.filters(),
        })
    }

    /// Parse message, read operations, and updated the database
    ///
    /// The following operations are supported:
    ///     - Query (create/drop database, create/drop table, alter table)
    ///     - Add (insert one or more entry into table)
    ///     - Update (update one or or table entries)
    ///     - Delete (delete one or more table entries)
    ///
    /// If a db-filter is provided, messages may be skipped by filter.
    /// Filters are applied as follows:
    ///  - no filters - all messages are processed
    ///  - include filters - only messages inside "include" filters are processed
    ///  - exclude filters - only messages outside of "exclude" filters are processed
    pub fn update_database(&mut self, json_msg: String) -> Result<(), Error> {
        let flv_message: FluvioMessage = serde_json::from_str(&json_msg)?;
        let cols = &flv_message.columns;
        let operation = &flv_message.operation;
        let uri_props = parse_uri(&flv_message.uri)?;

        if !allowed_by_filters(&self.filters, &uri_props.db_name) {
            println!(
                "msg for db '{}' skipped by filter",
                uri_props.db_name.unwrap_or_else(|| "".to_owned())
            );
            return Ok(());
        }
        println!("{:?}", &flv_message);
        println!("{:?}", &uri_props);

        match operation {
            Operation::Query(query) => self.send_query_op(&*query, uri_props)?,
            Operation::Add(data) => self.send_add_op(cols, &data, uri_props)?,
            Operation::Update(data) => self.send_update_op(cols, &data, uri_props)?,
            Operation::Delete(data) => self.send_delete_op(cols, &data, uri_props)?,
        }

        Ok(())
    }

    /// Send Query operation
    ///     - if "create database", do not switch to db as it does not exist.
    ///     - any other command, "switch to db first"
    fn send_query_op(&mut self, query: &str, uri_props: UriProps) -> Result<(), Error> {
        if !is_query_create_database(query) {
            self.switch_db_if_needed(&uri_props.db_name)?;
        }

        exec_query(&mut self.conn, query)
    }

    /// Add Rows into table operation
    ///
    /// Precondition:
    ///     - message must have columns
    ///
    /// 1) switch db
    /// 2) build "INSERT INTO" mysql operations
    /// 3) send operations to mysql
    ///
    fn send_add_op(
        &mut self,
        cols: &Option<Vec<String>>,
        data: &WriteRows,
        uri_props: UriProps,
    ) -> Result<(), Error> {
        check_valid_cols(cols)?;

        self.switch_db_if_needed(&uri_props.db_name)?;

        let table_name = table_name(&uri_props)?;
        let queries = build_add_queries(table_name, cols.as_ref().unwrap(), &data)?;
        for query in &queries {
            exec_query(&mut self.conn, query)?;
        }

        Ok(())
    }

    /// Update Rows in table operation
    ///
    /// Precondition:
    ///     - message must have columns
    ///
    /// 1) switch db
    /// 2) build "UPDATE" mysql operations
    /// 3) send operations to mysql
    ///
    fn send_update_op(
        &mut self,
        cols: &Option<Vec<String>>,
        data: &UpdateRows,
        uri_props: UriProps,
    ) -> Result<(), Error> {
        check_valid_cols(cols)?;

        self.switch_db_if_needed(&uri_props.db_name)?;

        let table_name = table_name(&uri_props)?;

        let queries = build_update_queries(table_name, &cols.as_ref().unwrap(), &data)?;
        for query in &queries {
            exec_query(&mut self.conn, query)?;
        }

        Ok(())
    }

    /// Delete Rows from table operation
    ///
    /// Precondition:
    ///     - message must have columns
    ///
    /// 1) switch db
    /// 2) build "DELETE" mysql operations
    /// 3) send operations to mysql
    ///
    fn send_delete_op(
        &mut self,
        cols: &Option<Vec<String>>,
        data: &DeleteRows,
        uri_props: UriProps,
    ) -> Result<(), Error> {
        check_valid_cols(cols)?;

        self.switch_db_if_needed(&uri_props.db_name)?;

        let table_name = table_name(&uri_props)?;

        let queries = build_delete_queries(table_name, &cols.as_ref().unwrap(), &data)?;
        for query in &queries {
            exec_query(&mut self.conn, query)?;
        }

        Ok(())
    }

    /// Each consumer operation may apply to different database.
    ///     - switch to new database if different from current database.
    fn switch_db_if_needed(&mut self, db_name: &Option<String>) -> Result<(), Error> {
        if self.db_name == *db_name {
            return Ok(()); // same db, do nothing
        }

        if let Some(db_name) = db_name {
            let result = self.conn.select_db(db_name);
            if !result {
                Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("cannot switch to db '{}'", db_name),
                ))
            } else {
                self.db_name = Some(db_name.clone());
                Ok(())
            }
        } else {
            Ok(())
        }
    }
}

/// Build "INSERT INTO" mysql operation and send to mysql server
fn build_add_queries(
    table_name: String,
    cols: &[String],
    data: &WriteRows,
) -> Result<Vec<String>, Error> {
    let columns = cols.join(", ");
    let mut queries: Vec<String> = vec![];

    for row in &data.rows {
        check_valid_col_count(row.cols.len(), cols.len())?;

        let row: Vec<String> = row.cols.iter().map(|val| val.to_string()).collect();
        queries.push(format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_name,
            columns,
            row.join(", ")
        ));
    }

    Ok(queries)
}

/// Build "UPDATE" mysql operation and send to mysql server
fn build_update_queries(
    table_name: String,
    cols: &[String],
    data: &UpdateRows,
) -> Result<Vec<String>, Error> {
    let preamble = format!("UPDATE {}", table_name);
    let mut queries: Vec<String> = vec![];

    for row in &data.rows {
        check_valid_col_count(row.before_cols.len(), cols.len())?;
        check_valid_col_count(row.after_cols.len(), cols.len())?;

        let mut set_cols: Vec<String> = vec![];
        let mut where_cols: Vec<String> = vec![];
        for (idx, before_value) in row.before_cols.iter().enumerate() {
            let after_value = &row.after_cols[idx];

            if before_value != after_value {
                set_cols.push(format!("{}={}", &cols[idx], after_value))
            }

            match before_value {
                Value::Null => where_cols.push(format!("{} is NULL", &cols[idx])),
                _ => where_cols.push(format!("{}={}", &cols[idx], before_value)),
            };
        }
        queries.push(format!(
            "{} SET {} WHERE {}",
            &preamble,
            set_cols.join(", "),
            where_cols.join(" AND ")
        ));
    }

    Ok(queries)
}

/// Build "DELETE" mysql operation and send to mysql server
fn build_delete_queries(
    table_name: String,
    cols: &[String],
    data: &DeleteRows,
) -> Result<Vec<String>, Error> {
    let preamble = format!("DELETE FROM {} WHERE", table_name);
    let mut queries: Vec<String> = vec![];

    for row in &data.rows {
        check_valid_col_count(row.cols.len(), cols.len())?;

        let mut where_cols: Vec<String> = vec![];
        for (idx, value) in row.cols.iter().enumerate() {
            match value {
                Value::Null => where_cols.push(format!("{} is NULL", &cols[idx])),
                _ => where_cols.push(format!("{}={}", &cols[idx], value)),
            };
        }
        queries.push(format!("{} {}", &preamble, where_cols.join(" AND ")));
    }

    Ok(queries)
}

/// Use mysql Connection to send query to mysql server and map any resulting errors
fn exec_query(conn: &mut Conn, query: &str) -> Result<(), Error> {
    println!("[query] {:?}", query);
    conn.query_drop(query)
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Query: {}", e)))
}

/// Ensure message has column object, return error otherwise
fn check_valid_cols(cols: &Option<Vec<String>>) -> Result<(), Error> {
    if cols.is_none() {
        Err(Error::new(
            ErrorKind::InvalidData,
            "expected column names, found none",
        ))
    } else {
        Ok(())
    }
}

/// Ensure URI table_name field, return error otherwise
fn table_name(uri_props: &UriProps) -> Result<String, Error> {
    if let Some(table_name) = &uri_props.table_name {
        Ok(table_name.clone())
    } else {
        Err(Error::new(
            ErrorKind::InvalidData,
            "expected table-name, found none",
        ))
    }
}

/// Ensure the number of columns match the number of value objects
fn check_valid_col_count(expected: usize, found: usize) -> Result<(), Error> {
    if found < expected {
        Err(Error::new(
            ErrorKind::InvalidData,
            format!("expected at least {} columns, found {}", expected, found),
        ))
    } else {
        Ok(())
    }
}

/// Parse URI for db_name & table_name
///
/// URI format
///   uri::/<db-label>/<db_name>/<table_name>
fn parse_uri(uri_str: &str) -> Result<UriProps, Error> {
    let uri = uri_str
        .parse::<Uri>()
        .map_err(|err| Error::new(ErrorKind::InvalidData, format!("{}", err)))?;
    let path: Vec<&str> = uri.path().split('/').collect();

    let db_name: Option<String> = if path.len() > 1 && !path[1].is_empty() {
        Some(path[1].to_owned())
    } else {
        None
    };

    let table_name: Option<String> = if path.len() > 2 && !path[2].is_empty() {
        Some(path[2].to_owned())
    } else {
        None
    };

    Ok(UriProps {
        db_name,
        table_name,
    })
}

/// Check if query is "CREATE DATABASE"
fn is_query_create_database(query: &str) -> bool {
    let query_uppercase = query.to_uppercase();
    query_uppercase.trim().contains("CREATE DATABASE")
}

/// Allowed by filters
///  - no filters  => true
///  - include filters => true for match, false otherwise
///  - exclude filters => false for match, true otherwise
fn allowed_by_filters(filters: &Option<Filters>, db_name: &Option<String>) -> bool {
    // disallow all entries without a database
    if db_name.is_none() {
        return false;
    }

    let db_name = db_name.as_ref().unwrap();
    if let Some(filters) = filters {
        match filters {
            Filters::Include { include_dbs: dbs } => dbs.contains(db_name),
            Filters::Exclude { exclude_dbs: dbs } => !dbs.contains(db_name),
        }
    } else {
        true
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::messages::{BeforeAfterCols, Cols, Value, WriteRows};

    #[test]
    fn test_build_add_queries() {
        let cols = vec![
            "name".to_owned(),
            "owner".to_owned(),
            "species".to_owned(),
            "sex".to_owned(),
            "birth".to_owned(),
            "death".to_owned(),
        ];

        let data = WriteRows {
            rows: vec![
                Cols {
                    cols: vec![
                        Value::String("Pip".to_owned()),
                        Value::String("Jake".to_owned()),
                        Value::String("mouse".to_owned()),
                        Value::String("m".to_owned()),
                        Value::Date {
                            year: 2020,
                            month: 3,
                            day: 30,
                        },
                        Value::Null,
                    ],
                },
                Cols {
                    cols: vec![
                        Value::String("Spot".to_owned()),
                        Value::String("Jane".to_owned()),
                        Value::String("dog".to_owned()),
                        Value::String("m".to_owned()),
                        Value::Date {
                            year: 2010,
                            month: 11,
                            day: 2,
                        },
                        Value::Null,
                    ],
                },
            ],
        };

        let table_name = "pet".to_owned();
        let result = build_add_queries(table_name, &cols, &data);
        if let Err(err) = &result {
            println!("Error: {}", err);
        }
        assert!(result.is_ok());

        let expected = vec![
            "INSERT INTO pet (name, owner, species, sex, birth, death) VALUES (\"Pip\", \"Jake\", \"mouse\", \"m\", \"2020-3-30\", Null)".to_owned(),
            "INSERT INTO pet (name, owner, species, sex, birth, death) VALUES (\"Spot\", \"Jane\", \"dog\", \"m\", \"2010-11-2\", Null)".to_owned()
        ];
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_build_update_queries() {
        let cols = vec![
            "name".to_owned(),
            "owner".to_owned(),
            "species".to_owned(),
            "sex".to_owned(),
            "birth".to_owned(),
            "death".to_owned(),
        ];

        let data = UpdateRows {
            rows: vec![
                BeforeAfterCols {
                    before_cols: vec![
                        Value::String("Pip".to_owned()),
                        Value::String("Jake".to_owned()),
                        Value::String("mouse".to_owned()),
                        Value::String("m".to_owned()),
                        Value::Date {
                            year: 2020,
                            month: 3,
                            day: 30,
                        },
                        Value::Null,
                    ],
                    after_cols: vec![
                        Value::String("Mickey".to_owned()),
                        Value::String("Jack".to_owned()),
                        Value::String("mouse".to_owned()),
                        Value::String("m".to_owned()),
                        Value::Date {
                            year: 2020,
                            month: 3,
                            day: 30,
                        },
                        Value::Null,
                    ],
                },
                BeforeAfterCols {
                    before_cols: vec![
                        Value::String("Spot".to_owned()),
                        Value::String("Jane".to_owned()),
                        Value::String("dog".to_owned()),
                        Value::String("m".to_owned()),
                        Value::Date {
                            year: 2010,
                            month: 11,
                            day: 2,
                        },
                        Value::Null,
                    ],
                    after_cols: vec![
                        Value::String("Spot".to_owned()),
                        Value::String("Jane".to_owned()),
                        Value::String("dog".to_owned()),
                        Value::String("m".to_owned()),
                        Value::Date {
                            year: 2010,
                            month: 11,
                            day: 2,
                        },
                        Value::Date {
                            year: 2020,
                            month: 6,
                            day: 10,
                        },
                    ],
                },
            ],
        };

        let table_name = "pet".to_owned();
        let result = build_update_queries(table_name, &cols, &data);
        if let Err(err) = &result {
            println!("Error: {}", err);
        }
        assert!(result.is_ok());

        let expected = vec![
            "UPDATE pet SET name=\"Mickey\", owner=\"Jack\" WHERE name=\"Pip\" AND owner=\"Jake\" AND species=\"mouse\" AND sex=\"m\" AND birth=\"2020-3-30\" AND death is NULL".to_owned(),
            "UPDATE pet SET death=\"2020-6-10\" WHERE name=\"Spot\" AND owner=\"Jane\" AND species=\"dog\" AND sex=\"m\" AND birth=\"2010-11-2\" AND death is NULL".to_owned(),
        ];
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_build_delete_queries() {
        let cols = vec![
            "name".to_owned(),
            "owner".to_owned(),
            "species".to_owned(),
            "sex".to_owned(),
            "birth".to_owned(),
            "death".to_owned(),
        ];

        let data = DeleteRows {
            rows: vec![
                Cols {
                    cols: vec![
                        Value::String("Pip".to_owned()),
                        Value::String("Jake".to_owned()),
                        Value::String("mouse".to_owned()),
                        Value::String("m".to_owned()),
                        Value::Date {
                            year: 2020,
                            month: 3,
                            day: 30,
                        },
                        Value::Null,
                    ],
                },
                Cols {
                    cols: vec![
                        Value::String("Spot".to_owned()),
                        Value::String("Jane".to_owned()),
                        Value::String("dog".to_owned()),
                        Value::String("m".to_owned()),
                        Value::Date {
                            year: 2010,
                            month: 11,
                            day: 2,
                        },
                        Value::Null,
                    ],
                },
            ],
        };

        let table_name = "pet".to_owned();
        let result = build_delete_queries(table_name, &cols, &data);
        if let Err(err) = &result {
            println!("Error: {}", err);
        }
        assert!(result.is_ok());

        let expected = vec![
            "DELETE FROM pet WHERE name=\"Pip\" AND owner=\"Jake\" AND species=\"mouse\" AND sex=\"m\" AND birth=\"2020-3-30\" AND death is NULL".to_owned(),
            "DELETE FROM pet WHERE name=\"Spot\" AND owner=\"Jane\" AND species=\"dog\" AND sex=\"m\" AND birth=\"2010-11-2\" AND death is NULL".to_owned(),
        ];
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_is_query_create_database() {
        let q = "create database flvTest".to_owned();
        assert_eq!(is_query_create_database(&q), true);

        let q = "CREATE database flvTest".to_owned();
        assert_eq!(is_query_create_database(&q), true);

        let q = " create database test if not exist".to_owned();
        assert_eq!(is_query_create_database(&q), true);

        let q = "CREATE TABLE pet (name VARCHAR(20), owner VARCHAR(20), species VARCHAR(20), sex CHAR(1), birth DATE)".to_owned();
        assert_eq!(is_query_create_database(&q), false);
    }

    #[test]
    fn test_allowed_by_filters() {
        // Test No db
        assert_eq!(allowed_by_filters(&None, &None), false);

        // Test No filters
        let db_name = "db-xx".to_owned();
        assert_eq!(allowed_by_filters(&None, &Some(db_name)), true);

        // Test Include filters
        let include_filters = Some(Filters::Include {
            include_dbs: vec!["db-10".to_owned(), "db-11".to_owned()],
        });

        let db_name = "db-xx".to_owned();
        assert_eq!(allowed_by_filters(&include_filters, &Some(db_name)), false);

        let db_name = "db-10".to_owned();
        assert_eq!(allowed_by_filters(&include_filters, &Some(db_name)), true);

        let db_name = "db-11".to_owned();
        assert_eq!(allowed_by_filters(&include_filters, &Some(db_name)), true);

        let db_name = "db-1111".to_owned();
        assert_eq!(allowed_by_filters(&include_filters, &Some(db_name)), false);

        let db_name = "db-20".to_owned();
        assert_eq!(allowed_by_filters(&include_filters, &Some(db_name)), false);

        // Test Exclude filters
        let exclude_filters = Some(Filters::Exclude {
            exclude_dbs: vec!["db-10".to_owned(), "db-11".to_owned()],
        });

        let db_name = "db-xx".to_owned();
        assert_eq!(allowed_by_filters(&exclude_filters, &Some(db_name)), true);

        let db_name = "db-10".to_owned();
        assert_eq!(allowed_by_filters(&exclude_filters, &Some(db_name)), false);

        let db_name = "db-11".to_owned();
        assert_eq!(allowed_by_filters(&exclude_filters, &Some(db_name)), false);

        let db_name = "db-1111".to_owned();
        assert_eq!(allowed_by_filters(&exclude_filters, &Some(db_name)), true);

        let db_name = "db-20".to_owned();
        assert_eq!(allowed_by_filters(&exclude_filters, &Some(db_name)), true);
    }

    #[test]
    fn test_parse_uri() {
        // Test both
        let uri = "flv://mysql.local/testdb/people".to_owned();
        let uri_props = parse_uri(&uri);
        assert!(uri_props.is_ok());

        let expected = UriProps {
            db_name: Some("testdb".to_owned()),
            table_name: Some("people".to_owned()),
        };
        assert_eq!(uri_props.unwrap(), expected);

        // Test Db-only
        let uri = "flv://mysql.local/flvTest".to_owned();
        let uri_props = parse_uri(&uri);
        assert!(uri_props.is_ok());

        let expected = UriProps {
            db_name: Some("flvTest".to_owned()),
            table_name: None,
        };
        assert_eq!(uri_props.unwrap(), expected);

        // Test none
        let uri = "flv://mysql.local/".to_owned();
        let uri_props = parse_uri(&uri);
        assert!(uri_props.is_ok());

        let expected = UriProps {
            db_name: None,
            table_name: None,
        };
        assert_eq!(uri_props.unwrap(), expected);
    }
}
