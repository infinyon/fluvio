//!
//! # Describe Template
//!
//!  Describe object boilerplate behind a trait.
//!

use std::sync::Arc;

use serde::Serialize;

use crate::t_println;

use super::OutputType;
use super::TableOutputHandler;
use super::KeyValOutputHandler;
use super::OutputError;
use super::Terminal;

pub trait DescribeObjectHandler {
    fn is_ok(&self) -> bool;
    fn is_error(&self) -> bool;
    fn validate(&self) -> Result<(), OutputError>;

    fn label() -> &'static str;
    fn label_plural() -> &'static str;
}

// -----------------------------------
// Data Structures
// -----------------------------------

pub struct DescribeObjectRender<O>(Arc<O>);

impl<O> DescribeObjectRender<O> {
    pub fn new(out: Arc<O>) -> Self {
        Self(out)
    }
}

// -----------------------------------
// Describe Objects Implementation
// -----------------------------------

impl<O: Terminal> DescribeObjectRender<O> {
    pub fn render<D>(&self, objects: &[D], output_type: OutputType) -> Result<(), OutputError>
    where
        D: DescribeObjectHandler + TableOutputHandler + KeyValOutputHandler + Serialize + Clone,
    {
        let out = self.0.clone();
        if output_type.is_table() {
            self.print_table(objects)
        } else {
            out.render_serde(&objects.to_vec(), output_type.into())
        }
    }

    pub fn print_table<D>(&self, objects: &[D]) -> Result<(), OutputError>
    where
        D: DescribeObjectHandler + TableOutputHandler + KeyValOutputHandler,
    {
        match objects.len() {
            1 => {
                let object = &objects[0];
                // provide detailed validation
                object.validate()?;
                self.0.render_key_values(object);
                self.0.clone().render_table(object, true);
            }

            _ => {
                // header
                println!("{}", self.header_summary(objects));

                for object in objects {
                    if object.is_ok() {
                        t_println!(self.0, "");
                        println!("{} DETAILS", D::label().to_ascii_uppercase());
                        println!("-------------");
                        self.0.render_key_values(object);
                        self.0.clone().render_table(object, true);
                    }
                }
            }
        }

        Ok(())
    }

    /// Provide a header summary
    fn header_summary<D>(&self, objects: &[D]) -> String
    where
        D: DescribeObjectHandler,
    {
        let all_cnt = objects.len();
        let mut ok_cnt = 0;
        let mut err_cnt = 0;
        for object in objects {
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
                ok_cnt,
                all_cnt,
                D::label_plural(),
                invalid_cnt
            )
        } else {
            format!(
                "Retrieved {} out of {} {}",
                ok_cnt,
                all_cnt,
                D::label_plural()
            )
        }
    }
}
