use std::sync::Arc;

use comfy_table::Row;
use comfy_table::Table;

use crate::t_println;
use super::Terminal;
use crate::output::DisplayTable;

pub trait TableOutputHandler {
    fn header(&self) -> Row;
    fn content(&self) -> Vec<Row>;
    fn errors(&self) -> Vec<String>;
}

pub struct TableRenderer<O>(Arc<O>);

impl<O> TableRenderer<O>
where
    O: Terminal,
{
    pub fn new(out: Arc<O>) -> Self {
        Self(out)
    }

    pub fn render<T>(&self, list: &T, indent: bool)
    where
        T: TableOutputHandler,
    {
        // expecting array with one or more elements
        self.display_errors(list);
        self.display_table(list, indent);
    }

    // display errors one at a time
    fn display_errors<T: TableOutputHandler>(&self, list: &T) {
        if !list.errors().is_empty() {
            for error in list.errors() {
                t_println!(self.0, "{}", error);
            }
            t_println!(self.0, "-------------");
        }
    }

    /// convert result to table output and print to screen
    fn display_table<T>(&self, list: &T, indent: bool)
    where
        T: TableOutputHandler,
    {
        let header = list.header();
        let content = list.content();

        // if table is empty, return
        if content.is_empty() {
            return;
        }

        // Create the table
        let mut table = Table::new();

        // add header
        table.set_header(header);

        // add rows
        for row in content {
            table.add_row(row);
        }

        table.load_preset(comfy_table::presets::NOTHING);

        // generate table padding
        let pad_left: u8 = if indent { 5 } else { 1 };

        // print table to stdout
        table.print_std(pad_left);
    }
}
