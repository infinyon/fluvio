//!
//! # Describe Template
//!
//!  Describe object boilerplate behind a trait.
//!

use serde::Serialize;

use crate::error::CliError;
use crate::common::{KeyValOutputHandler, TableOutputHandler};

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Serialize, Debug)]
pub struct DescribeObjects<T> {
    pub label: &'static str,
    pub label_plural: &'static str,

    pub describe_objects: Vec<T>,
}

// -----------------------------------
// Describe Object Trait
// -----------------------------------

pub trait DescribeObjectHandler {
    fn is_ok(&self) -> bool;
    fn is_error(&self) -> bool;

    fn validate(&self) -> Result<(), CliError>;
}

// -----------------------------------
// Describe Objects Implementation
// -----------------------------------

impl<T> DescribeObjects<T>
where
    T: DescribeObjectHandler + KeyValOutputHandler + TableOutputHandler,
{
    /// Process server based on output type
    pub fn print_table(&self) -> Result<(), CliError> {
        match self.objects_cnt() {
            1 => {
                let object = &self.describe_objects[0];
                // provide detailed validation
                object.validate()?;
                object.display_keyvals();
                object.display_table(true);
            }

            _ => {
                // header
                println!("{}", self.header_summary());

                // ob ject
                for object in &self.describe_objects {
                    if object.is_ok() {
                        println!("");
                        println!("{} DETAILS", self.label.to_ascii_uppercase());
                        println!("-------------");
                        object.display_keyvals();
                        object.display_table(true);
                    }
                }
            }
        }

        Ok(())
    }

    /// Provide a header summary
    fn header_summary(&self) -> String {
        let all_cnt = self.describe_objects.len();
        let mut ok_cnt = 0;
        let mut err_cnt = 0;
        for object in &self.describe_objects {
            if object.is_ok() {
                ok_cnt += 1;
            } else if object.is_error() {
                err_cnt += 1;
            }
        }

        let invalid_cnt = all_cnt - (ok_cnt + err_cnt);
        if invalid_cnt > 0 {
            format!(
                "Retrieved {} out of {} {} ({} are invalid)",
                ok_cnt, all_cnt, self.label_plural, invalid_cnt
            )
        } else {
            format!(
                "Retrieved {} out of {} {}",
                ok_cnt, all_cnt, self.label_plural
            )
        }
    }

    /// retrieves objects count
    fn objects_cnt(&self) -> usize {
        self.describe_objects.len()
    }
}

