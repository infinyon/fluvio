use mysql::prelude::*;
use mysql::{from_value, Conn, OptsBuilder, Row};

use crate::producer::Database;
use crate::error::CdcError;

pub fn get_opts(db_params: &Database) -> OptsBuilder {
    OptsBuilder::new()
        .ip_or_hostname(db_params.ip_or_host())
        .tcp_port(db_params.port())
        .user(db_params.user())
        .pass(db_params.password())
}

pub fn get_table_columns(
    db_name: &str,
    table_name: &str,
    db_params: &Database,
) -> Result<Vec<String>, CdcError> {
    let opts = get_opts(db_params);
    let mut conn = Conn::new(opts)?;
    let query = format!(
        r"SELECT COLUMN_NAME FROM information_schema.columns
            WHERE table_schema='{}' AND table_name='{}'
            ORDER BY ORDINAL_POSITION",
        db_name, table_name
    );

    let rows: Vec<Row> = conn.query(query)?;
    let columns = rows
        .into_iter()
        .map(|r| from_value(r.unwrap()[0].clone()))
        .collect();

    Ok(columns)
}
