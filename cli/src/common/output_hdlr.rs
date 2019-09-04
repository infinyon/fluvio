//!
//! # Output Handlers
//!
//! Output Type defines the types of output allowed.
//! Table and Encoding Traits encodes and prints to screen.
//!
use std::io::Error as IoError;
use std::io::ErrorKind;

use prettytable::format;
use prettytable::Row;
use prettytable::Table;
use prettytable::row;
use prettytable::cell;
use serde::Serialize;
use structopt::clap::arg_enum;

use crate::error::CliError;

// -----------------------------------
// Output Types
// -----------------------------------

// Uses clap::arg_enum to choose possible variables
arg_enum! {
    #[derive(Debug, Clone, PartialEq)]
    #[allow(non_camel_case_types)]
    pub enum OutputType {
        table,
        yaml,
        json,
    }
}

/// OutputType defaults to table formatting
impl ::std::default::Default for OutputType {
    fn default() -> Self {
        OutputType::table
    }
}

/// OutputType check if table
impl OutputType {
    pub fn is_table(&self) -> bool {
        *self == OutputType::table
    }
}

// -----------------------------------
// Table Handler Trait
// -----------------------------------

pub trait TableOutputHandler {
    fn header(&self) -> Row;
    fn content(&self) -> Vec<Row>;
    fn errors(&self) -> Vec<String>;

    // display errors one at a time
    fn display_errors(&self) {
        if self.errors().len() > 0 {
            for error in self.errors() {
                println!("{}", error);
            }
            println!("-------------");
        }
    }

    /// convert result to table output and print to screen
    fn display_table(&self, indent: bool) {
        let header = self.header();
        let content = self.content();

        // if table is empty, return
        if content.len() == 0 {
            return;
        }

        // Create the table
        let mut table = Table::new();
        let mut format = format::consts::FORMAT_CLEAN.clone();
        let pad_left = if indent { 5 } else { 1 };
        format.padding(pad_left, 1);
        table.set_format(format);

        // add header
        table.set_titles(header);

        // add rows
        for row in content {
            table.add_row(row);
        }

        // print table to stdout
        table.printstd();
    }
}

// -----------------------------------
// Key/Value Handler Trait
// -----------------------------------

pub trait KeyValOutputHandler {
    fn key_vals(&self) -> Vec<(String, Option<String>)>;

    /// convert result to table output and print to screen
    fn display_keyvals(&self) {
        let key_vals = self.key_vals();

        // Create the table
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_CLEAN);

        for (key, val_opt) in key_vals {
            if let Some(val) = val_opt {
                table.add_row(row!(key, ":".to_owned(), val));
            } else {
                table.add_row(row!(key));
            }
        }

        // print table to stdout
        table.printstd();
    }
}

// -----------------------------------
// Encoder Handler Trait
// -----------------------------------

pub trait EncoderOutputHandler {
    type DataType: Serialize;

    /// references data type
    fn data(&self) -> &Self::DataType;

    /// output data based on output type
    fn display_encoding(&self, output_type: &OutputType) -> Result<(), CliError> {
        match output_type {
            OutputType::yaml => self.to_yaml(),
            OutputType::json => self.to_json(),
            _ => Err(CliError::IoError(IoError::new(
                ErrorKind::Other,
                "unknown encoding type",
            ))),
        }
    }

    /// convert result to json format and print to screen
    fn to_json(&self) -> Result<(), CliError>
    where
        Self::DataType: Serialize,
    {
        let data: &Self::DataType = self.data();
        let serialized = serde_json::to_string_pretty(data).unwrap();

        println!(" {}", serialized);

        Ok(())
    }

    /// convert result to yaml format and print to screen
    fn to_yaml(&self) -> Result<(), CliError>
    where
        Self::DataType: Serialize,
    {
        let data: &Self::DataType = self.data();
        let serialized = serde_yaml::to_string(data).unwrap();

        println!("{}", serialized);

        Ok(())
    }
}
