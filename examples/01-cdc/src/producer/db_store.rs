use std::collections::BTreeMap;

use crate::producer::Database;
use crate::producer::mysql::get_table_columns;
use crate::error::CdcError;

type DbName = String;
type TableName = String;
type Column = String;
#[derive(Debug)]
pub struct DbStore {
    dbs: BTreeMap<DbName, TableStore>,
    db_params: Database,
}

#[derive(Debug)]
pub struct TableStore {
    tables: BTreeMap<TableName, Vec<Column>>,
}

impl TableStore {
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
    }
}

impl Default for TableStore {
    fn default() -> Self {
        Self::new()
    }
}

impl DbStore {
    pub fn new(db_params: &Database) -> Self {
        Self {
            dbs: BTreeMap::new(),
            db_params: db_params.clone(),
        }
    }

    pub fn save_columns(&mut self, db_name: &str, table_name: &str, columns: &[Column]) {
        let table_store = match self.dbs.get_mut(db_name) {
            Some(table_store) => table_store,
            None => {
                self.dbs.insert(db_name.to_string(), TableStore::new());
                self.dbs.get_mut(db_name).unwrap()
            }
        };

        table_store
            .tables
            .insert(table_name.to_string(), columns.to_owned());
    }

    #[allow(dead_code)]
    pub fn has_columns(&self, db_name: &str, table_name: &str) -> bool {
        if let Some(table_store) = self.dbs.get(db_name) {
            table_store.tables.contains_key(table_name)
        } else {
            false
        }
    }

    pub fn get_columns(
        &mut self,
        db_name: &str,
        table_name: &str,
    ) -> Result<Vec<String>, CdcError> {
        // get from local store (if available)
        if let Some(table_store) = self.dbs.get(db_name) {
            if let Some(cols) = table_store.tables.get(table_name) {
                return Ok(cols.clone());
            }
        };

        let columns = get_table_columns(db_name, table_name, &self.db_params)?;
        self.save_columns(db_name, table_name, &columns);
        Ok(columns)
    }

    pub fn clear_columns(&mut self, db_name: &str, table_name: &str) {
        if let Some(table_store) = self.dbs.get_mut(db_name) {
            table_store.tables.remove(table_name);
        }
    }
}

#[cfg(test)]
mod test {
    use crate::producer::Database;

    use super::DbStore;

    #[test]
    fn test_db_store() {
        let database = Database::default();
        let mut db_store = DbStore::new(&database);
        let db = "flvTest".to_owned();
        let pet_table = "pet".to_owned();
        let user_table = "user".to_owned();

        // test - add pet columns
        let some_pet_columns = vec![
            "name".to_owned(),
            "owner".to_owned(),
            "species".to_owned(),
            "sex".to_owned(),
            "birth".to_owned(),
            "death".to_owned(),
        ];
        db_store.save_columns(&db, &pet_table, &some_pet_columns);
        assert_eq!(db_store.has_columns(&db, &pet_table), true);

        let columns = db_store.get_columns(&db, &pet_table);
        assert!(columns.is_ok());
        assert_eq!(columns.unwrap(), some_pet_columns);

        // test - update pet columns
        let other_pet_columns = vec![
            "name".to_owned(),
            "kind".to_owned(),
            "sex".to_owned(),
            "birth".to_owned(),
            "death".to_owned(),
        ];
        db_store.save_columns(&db, &pet_table, &other_pet_columns);
        assert_eq!(db_store.has_columns(&db, &pet_table), true);

        let columns = db_store.get_columns(&db, &pet_table);
        assert!(columns.is_ok());
        assert_eq!(columns.unwrap(), other_pet_columns);

        // test - add user columns
        let some_user_columns = vec![
            "first_name".to_owned(),
            "last_name".to_owned(),
            "sex".to_owned(),
            "birth".to_owned(),
        ];
        db_store.save_columns(&db, &user_table, &some_user_columns);
        assert_eq!(db_store.has_columns(&db, &user_table), true);
        assert_eq!(db_store.has_columns(&db, &pet_table), true);

        let columns = db_store.get_columns(&db, &user_table);
        assert!(columns.is_ok());
        assert_eq!(columns.unwrap(), some_user_columns);

        let store_dump = format!("{:?}", db_store.dbs);
        assert_eq!(store_dump, "{\"flvTest\": TableStore { tables: {\"pet\": [\"name\", \"kind\", \"sex\", \"birth\", \"death\"], \"user\": [\"first_name\", \"last_name\", \"sex\", \"birth\"]} }}");

        // test - clear user columns
        db_store.clear_columns(&db, &user_table);
        assert_eq!(db_store.has_columns(&db, &user_table), false);
        assert_eq!(db_store.has_columns(&db, &pet_table), true);

        let store_dump = format!("{:?}", db_store.dbs);
        assert_eq!(store_dump, "{\"flvTest\": TableStore { tables: {\"pet\": [\"name\", \"kind\", \"sex\", \"birth\", \"death\"]} }}");

        //test clear pet columns
        db_store.clear_columns(&db, &pet_table);
        assert_eq!(db_store.has_columns(&db, &user_table), false);
        assert_eq!(db_store.has_columns(&db, &pet_table), false);

        let store_dump = format!("{:?}", db_store.dbs);
        assert_eq!(store_dump, "{\"flvTest\": TableStore { tables: {} }}");

        // add pet one more time.
        db_store.save_columns(&db, &pet_table, &some_pet_columns);

        let store_dump = format!("{:?}", db_store.dbs);
        assert_eq!(store_dump, "{\"flvTest\": TableStore { tables: {\"pet\": [\"name\", \"owner\", \"species\", \"sex\", \"birth\", \"death\"]} }}");
    }
}
