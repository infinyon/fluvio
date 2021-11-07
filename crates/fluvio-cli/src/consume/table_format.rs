// -----------------------------------
//  Basic Table
// -----------------------------------

///// Print records in text-based table format
//pub fn print_table_record(record: &[u8], count: i32) -> String {
//    use prettytable::{Row, cell, Cell, Slice};
//    use prettytable::format::{self, FormatBuilder};
//
//    let maybe_json: serde_json::Value = match serde_json::from_slice(record) {
//        Ok(value) => value,
//        Err(_e) => panic!("Value not json"),
//    };
//
//    let obj = maybe_json.as_object().unwrap();
//
//    // This is the case where we don't provide any table info. We want to print a table w/ all top-level keys as headers
//    // Think about how we might only select specific keys
//    let keys_str: Vec<String> = obj.keys().map(|k| k.to_string()).collect();
//
//    // serde_json's Value::String() gets wrapped in quotes if we use `to_string()`
//    let values_str: Vec<String> = obj
//        .values()
//        .map(|v| {
//            if v.is_string() {
//                v.as_str()
//                    .expect("Value not representable as str")
//                    .to_string()
//            } else {
//                v.to_string()
//            }
//        })
//        .collect();
//
//    let header: Row = Row::new(keys_str.iter().map(|k| cell!(k.to_owned())).collect());
//    let entries: Row = Row::new(values_str.iter().map(|v| Cell::new(v)).collect());
//
//    // Print the table
//    let t_print = vec![header, entries];
//
//    let mut table = prettytable::Table::init(t_print);
//
//    let base_format: FormatBuilder = (*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR).into();
//    let table_format = base_format;
//    table.set_format(table_format.build());
//
//    // FIXME: Live display of table data easily misaligns column widths
//    // if there is a length diff between the header and the data
//    // The rows after the first (count == 0) don't line up with the header
//    // prettytable might not support the live display use-case we want
//    if count == 0 {
//        table.printstd();
//    } else {
//        let slice = table.slice(1..);
//        slice.printstd();
//    }
//    format!("")
//}

// -----------------------------------
//  Fancy Table
// -----------------------------------

// -----------------------------------
//  Utilities
// -----------------------------------

use crate::Result;
use tui::widgets::TableState;
//use std::io::Stdout;
//use tui::{
//    backend::CrosstermBackend,
//    layout::{Constraint, Layout},
//    style::{Color, Modifier, Style},
//    widgets::{Block, Borders, Cell, Row, Table},
//    Frame, Terminal,
//};
//use crossterm::event::{Event, KeyCode, MouseEventKind};

#[derive(Debug, Default, Clone)]
pub struct TableModel {
    pub state: TableState,
    pub headers: Vec<String>,
    pub data: Vec<Vec<String>>,
    // primary key: set of keys to select when deciding to update table view: Default on 1st header key
    // column display ordering rules: alphabetical, manual: Default alphabetical
    // toggle for update-row vs append-row
    // display-cache time?
}

impl TableModel {
    // I think this should accept headers that don't exist in the data. Print empty columns
    pub fn update_header(&mut self, headers: Vec<String>) -> Result<()> {
        self.headers = headers;

        Ok(())
    }

    // TODO: When we support manual column ordering
    // I think this should accept headers that don't exist in the data. Just ignore the keys that don't exist. Don't error
    //pub fn update_ordering(&mut self, headers: Vec<String>) -> Result<()> {}

    // We should support a pure append-only workflow if we know that's what we want
    //pub fn insert_row(&mut self, row: Vec<String>) -> Result<()> { }

    // For now, this will look for the left-most column and if found, update that row
    // Appends row if not found
    // Issue: When we read in json data, it is sorted by key in alphanumeric order, so left-most will always be the alphabetical 1st
    // This might cause issues w/r/t updates, since we treat 1st column as primary
    pub fn update_row(&mut self, row: Vec<String>) -> Result<()> {
        let mut found = None;

        for (index, r) in self.data.iter().enumerate() {
            if r[0] == row[0] {
                found = Some(index);
                break;
            }
        }

        if let Some(row_index) = found {
            self.data[row_index] = row;
        } else {
            self.data.push(row);
        }

        Ok(())
    }

    //TODO
    //pub fn delete_row(&mut self, row_index: usize) -> Result<()> {}

    //pub fn num_columns(&self) -> usize {
    //    self.headers.len()
    //}

    //pub fn current_selected(&self) -> usize {
    //    self.state.selected().unwrap_or(0)
    //}

    //pub fn next(&mut self) {
    //    let i = match self.state.selected() {
    //        Some(i) => {
    //            if i >= self.data.len() - 1 {
    //                0
    //            } else {
    //                i + 1
    //            }
    //        }
    //        None => 0,
    //    };
    //    self.state.select(Some(i));
    //}

    //pub fn previous(&mut self) {
    //    let i = match self.state.selected() {
    //        Some(i) => {
    //            if i == 0 {
    //                self.data.len() - 1
    //            } else {
    //                i - 1
    //            }
    //        }
    //        None => 0,
    //    };
    //    self.state.select(Some(i));
    //}

    //pub fn first(&mut self) {
    //    self.state.select(Some(0));
    //}

    //pub fn last(&mut self) {
    //    self.state.select(Some(self.data.len() - 1));
    //}

    //pub fn event_handler(&mut self, user_input: Event) -> TableEvent {
    //    if let Event::Key(key) = user_input {
    //        match key.code {
    //            KeyCode::Char('q') | KeyCode::Esc => TableEvent::Terminate,
    //            KeyCode::Char('c') => {
    //                self.data = Vec::new();
    //                self.state.select(None);
    //                TableEvent::InputHandled(user_input)
    //            }
    //            KeyCode::Up => {
    //                self.previous();
    //                TableEvent::InputHandled(user_input)
    //            }
    //            KeyCode::Down => {
    //                self.next();
    //                TableEvent::InputHandled(user_input)
    //            }
    //            KeyCode::Home => {
    //                self.first();
    //                TableEvent::InputHandled(user_input)
    //            }
    //            KeyCode::End => {
    //                self.last();
    //                TableEvent::InputHandled(user_input)
    //            }
    //            _ => TableEvent::InputIgnored(user_input),
    //        }
    //    } else if let Event::Mouse(event) = user_input {
    //        match event.kind {
    //            MouseEventKind::ScrollDown => {
    //                self.next();
    //                TableEvent::InputHandled(user_input)
    //            }
    //            MouseEventKind::ScrollUp => {
    //                self.previous();
    //                TableEvent::InputHandled(user_input)
    //            }
    //            _ => TableEvent::InputIgnored(user_input),
    //        }
    //    } else {
    //        TableEvent::InputIgnored(user_input)
    //    }
    //}

    //// This will take a full screen buffer
    ///// Print records in table format
    /////
    ///// If you do not provide any column ordering, it will be alphabetized by the top-level keys
    //pub fn render(
    //    &mut self,
    //    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    //    // primary_key: Option<String>,
    //    // column_order: Option<Vec<String>,
    //    //record: &[u8],
    //) {
    //    terminal
    //        .draw(|frame| self.table_ui(frame))
    //        .expect("Could not render table frame");
    //}

    //pub fn table_ui(&mut self, f: &mut Frame<CrosstermBackend<Stdout>>) {
    //    let rects = Layout::default()
    //        .constraints([Constraint::Percentage(100)].as_ref())
    //        .margin(5)
    //        .split(f.size());

    //    // Calculate the widths based on # of columns
    //    let equal_column_width = (100 / self.num_columns()) as u16;

    //    let mut column_constraints: Vec<Constraint> = Vec::new();

    //    // Define the widths of the columns
    //    for _ in 0..self.num_columns() {
    //        column_constraints.push(Constraint::Percentage(equal_column_width));
    //    }

    //    let selected_symbol = format!("{} >> ", self.current_selected());

    //    let selected_style = Style::default().add_modifier(Modifier::REVERSED);
    //    let normal_style = Style::default().bg(Color::Blue);
    //    let header_cells = self
    //        .headers
    //        .iter()
    //        .map(|h| Cell::from(h.as_str()).style(Style::default().fg(Color::Red)));
    //    let header = Row::new(header_cells)
    //        .style(normal_style)
    //        .height(1)
    //        .bottom_margin(1);
    //    let rows = self.data.iter().map(|item| {
    //        let height = item
    //            .iter()
    //            .map(|content| content.chars().filter(|c| *c == '\n').count())
    //            .max()
    //            .unwrap_or(0)
    //            + 1;
    //        let cells = item.iter().map(|c| Cell::from(c.as_str()));
    //        Row::new(cells).height(height as u16).bottom_margin(1)
    //    });
    //    let t = Table::new(rows)
    //        .header(header)
    //        .block(Block::default().borders(Borders::ALL).title(format!(
    //            "('c' to clear table | 'q' or ESC to exit) | Items: {}",
    //            self.data.len()
    //        )))
    //        .highlight_style(selected_style)
    //        .highlight_symbol(selected_symbol.as_str())
    //        .widths(&column_constraints);
    //    f.render_stateful_widget(t, rects[0], &mut self.state);
    //}
}

//#[derive(Debug)]
//pub enum TableEvent {
//    InputHandled(Event),
//    InputIgnored(Event),
//    Terminate,
//}
