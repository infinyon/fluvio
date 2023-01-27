// -----------------------------------
//  Full Table
// -----------------------------------

use std::io::Stdout;
use std::collections::BTreeMap;

use tui::widgets::TableState;
use tui::{
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Row, Table},
    Frame, Terminal,
};
use crossterm::event::{Event, KeyCode, MouseEventKind};
use anyhow::Result;

use fluvio::metadata::tableformat::{TableFormatColumnConfig, TableFormatSpec, DataFormat};

#[derive(Debug, Default, Clone)]
pub struct TableModel {
    state: TableState,
    columns: Vec<TableFormatColumnConfig>, // List of json key paths. Should be initialized either at Self::new() or at first row entered

    // Maybe data should be some kind of map structure, so we can enforce headers as column order easier
    data: Vec<BTreeMap<String, String>>,
    input_format: DataFormat,
    // display-cache time?
}

impl TableModel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn columns(&self) -> Vec<TableFormatColumnConfig> {
        self.columns.clone()
    }

    pub fn _data(&self) -> Vec<BTreeMap<String, String>> {
        self.data.clone()
    }

    pub fn with_tableformat(&mut self, tableformat: Option<TableFormatSpec>) {
        if let Some(format) = tableformat {
            if let Some(input_format) = format.input_format {
                self.input_format = input_format;
            }
            if let Some(columns) = format.columns {
                self.columns = columns;
            }
        }
    }

    // I think this should accept columns that don't exist in the data. Print empty columns
    pub fn update_columns(&mut self, columns: Vec<TableFormatColumnConfig>) -> Result<()> {
        self.columns = columns;

        Ok(())
    }

    // By default, we append rows
    // To in-place update rows, create a tableformat and define what columns are your primary keys
    // Then new_data is compared against all the table data
    // The comparison is for a row where the values at the primary keys match the new_data values
    // If found, that row is replaced with new_data values
    pub fn update_row(&mut self, new_data: BTreeMap<String, String>) -> Result<()> {
        // For the first row, we know we can just insert data
        if self.data.is_empty() {
            self.data.push(new_data);
            return Ok(());
        }

        // Get a list of the primary keys
        // If we don't have a primary key set, the default behavior is to append the row
        let all_primary_keys = self.get_primary_keys();
        //println!("Primary key: {:?}", primary_keys);

        if let Some(primary_keys) = all_primary_keys {
            let mut append_row = true;

            // use the primary keys when evaluating a row update
            for (index, row) in self.data.iter().enumerate() {
                // Look over the primary keys to determine if we update the row

                // Reset flag for next row processing
                append_row = false;

                for (index, key) in primary_keys.iter().enumerate() {
                    match row.get(key.as_str()) {
                        Some(value) => {
                            if let Some(new_data_value) = new_data.get(key.as_str()) {
                                // primary key value, and new_data values don't match
                                if value != new_data_value {
                                    append_row = true;
                                    continue;
                                } else {
                                    // If this is the last primary key we're checking
                                    // Break out of the primary key loop
                                    if (index + 1) == self.data.len() {
                                        append_row = false;
                                        break;
                                    }
                                }
                            } else {
                                // key doesn't exist in new_data
                                append_row = true;
                            }
                        }
                        None => {
                            // key doesn't exist in row
                            append_row = true;
                        }
                    }
                }

                // If the values for the primary keys match, then update the row
                if !append_row {
                    self.data[index] = new_data.clone();
                    break;
                }
            }

            // Otherwise append the data to the end after we look at all the rows
            if append_row {
                self.data.push(new_data);
            }
        } else {
            // Append data
            self.data.push(new_data);
        }

        Ok(())
    }

    //TODO: Seems like we're going to need this, but not now
    //pub fn delete_row(&mut self, row_index: usize) -> Result<()> {}

    /// Return number of rows in cache
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    // The purpose of the primary keys is to control whether a record
    // is appended or updated based on matching values on the list of keys
    pub fn get_primary_keys(&self) -> Option<Vec<String>> {
        if !self.columns.is_empty() {
            let mut primary_keys = Vec::new();

            for c in &self.columns {
                if let Some(is_primary_key) = c.primary_key {
                    if is_primary_key {
                        primary_keys.push(c.key_path.clone());
                    }
                }
            }

            if !primary_keys.is_empty() {
                Some(primary_keys)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Return the row selected
    pub fn current_selected(&self) -> usize {
        self.state.selected().unwrap_or(0)
    }

    /// Select next row from current selection. Wrap to top
    pub fn next(&mut self) {
        if !self.data.is_empty() {
            let i = match self.state.selected() {
                Some(i) => {
                    if i >= self.data.len() - 1 {
                        0
                    } else {
                        i + 1
                    }
                }
                None => 0,
            };
            self.state.select(Some(i));
        }
    }

    /// Select previous row from current selection. Wrap to bottom.
    pub fn previous(&mut self) {
        if !self.data.is_empty() {
            let i = match self.state.selected() {
                Some(i) => {
                    if i == 0 {
                        self.data.len() - 1
                    } else {
                        i - 1
                    }
                }
                None => 0,
            };
            self.state.select(Some(i));
        }
    }

    /// Select first row in data
    pub fn first(&mut self) {
        if !self.data.is_empty() {
            self.state.select(Some(0));
        }
    }

    /// Select last row in data
    pub fn last(&mut self) {
        if !self.data.is_empty() {
            self.state.select(Some(self.data.len() - 1));
        }
    }

    // Skip up 5 rows, no wrap
    pub fn page_up(&mut self) {
        if !self.data.is_empty() {
            if self.data.len() > 5 {
                let i = match self.state.selected() {
                    Some(i) => {
                        let up_five: isize = (i as isize) - 5;

                        if up_five >= 0 {
                            up_five as usize
                        } else {
                            0
                        }
                    }
                    None => 0,
                };
                self.state.select(Some(i));
            } else {
                self.state.select(Some(0));
            }
        }
    }

    // Skip down 5 rows, no wrap
    pub fn page_down(&mut self) {
        if !self.data.is_empty() {
            if self.data.len() > 5 {
                let i = match self.state.selected() {
                    Some(i) => {
                        let down_five = i + 5;

                        if down_five <= self.data.len() {
                            down_five
                        } else {
                            self.data.len() - 1
                        }
                    }
                    None => 0,
                };
                self.state.select(Some(i));
            } else {
                self.state.select(Some(self.data.len() - 1));
            }
        }
    }

    /// Takes in `user_input` and triggers a render to the table
    /// Returns the appropriate `TableEventResponse`
    pub fn event_handler(
        &mut self,
        user_input: Event,
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    ) -> TableEventResponse {
        let response;
        if let Event::Key(key) = user_input {
            response = match key.code {
                KeyCode::Char('q') | KeyCode::Esc => TableEventResponse::Terminate,
                KeyCode::Char('c') => {
                    self.data = Vec::new();
                    self.state.select(None);
                    TableEventResponse::InputHandled(user_input)
                }
                KeyCode::Up => {
                    self.previous();
                    TableEventResponse::InputHandled(user_input)
                }
                KeyCode::Down => {
                    self.next();
                    TableEventResponse::InputHandled(user_input)
                }
                KeyCode::Home => {
                    self.first();
                    TableEventResponse::InputHandled(user_input)
                }
                KeyCode::End => {
                    self.last();
                    TableEventResponse::InputHandled(user_input)
                }
                KeyCode::PageUp => {
                    self.page_up();
                    TableEventResponse::InputHandled(user_input)
                }
                KeyCode::PageDown => {
                    self.page_down();
                    TableEventResponse::InputHandled(user_input)
                }
                _ => TableEventResponse::InputIgnored(user_input),
            }
        } else if let Event::Mouse(event) = user_input {
            response = match event.kind {
                MouseEventKind::ScrollDown => {
                    self.next();
                    TableEventResponse::InputHandled(user_input)
                }
                MouseEventKind::ScrollUp => {
                    self.previous();
                    TableEventResponse::InputHandled(user_input)
                }
                _ => TableEventResponse::InputIgnored(user_input),
            }
        } else {
            response = TableEventResponse::InputIgnored(user_input)
        }

        self.render(terminal);
        response
    }

    // This will take a full screen buffer
    /// Print records in table format
    ///
    /// If you do not provide any column ordering, it will be alphabetized by the top-level keys
    pub fn render(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<Stdout>>,
        // primary_key: Option<String>,
        // column_order: Option<Vec<String>,
        //record: &[u8],
    ) {
        if terminal.draw(|frame| self.table_ui(frame)).is_err() {
            println!("Could not render table frame")
        }
    }

    /// Render the frame for the table
    /// Re-calculates the table frame so it can be drawn on screen
    pub fn table_ui(&mut self, f: &mut Frame<CrosstermBackend<Stdout>>) {
        let rects = Layout::default()
            .constraints([Constraint::Percentage(100)].as_ref())
            .margin(0)
            .split(f.size());

        // Calculate the widths based on # of columns
        let equal_column_width = if self.num_columns() > 1 {
            (100 / self.num_columns()) as u16
        } else {
            // If you're here, there isn't any headers set. Likely no data too.
            // This is just going to prevent immediate panic from division by zero
            100
        };

        let mut column_constraints: Vec<Constraint> = Vec::new();

        // TODO: Maybe provide a way to size the width of a column
        // Define the widths of the columns
        for _ in 0..self.num_columns() {
            column_constraints.push(Constraint::Percentage(equal_column_width));
        }

        let selected_symbol = format!("{} >> ", self.current_selected());

        let selected_style = Style::default().add_modifier(Modifier::REVERSED);

        let header_cells = self.columns.iter().map(|column| {
            // If the column has an alternative label, use that
            // Otherwise, use the key path
            let header_label = if let Some(label) = column.header_label.clone() {
                label
            } else {
                // TODO: Test this with a nested key.
                // We probably want to use the inner-most key in the path if no label given
                column.key_path.clone()
            };

            Cell::from(header_label)
        });

        let header_style = Style::default()
            .bg(Color::Blue)
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD);
        let header = Row::new(header_cells)
            .style(header_style)
            .height(1)
            .bottom_margin(0);

        // render rows based on header order
        let mut rows = vec![];

        for (_index, row_data) in self.data.iter().enumerate() {
            let mut cells = vec![];
            for col in &self.columns {
                let key_path = col.key_path.as_str();
                let value = if let Some(v) = row_data.get(key_path) {
                    v
                } else {
                    // If the user provides a key that doesn't exist
                    // we want to print a column, but leave the value blank
                    ""
                };

                cells.push(Cell::from(value));
            }

            //rows.push(Row::new(cells).height(height as u16).bottom_margin(0))
            rows.push(Row::new(cells).height(1).bottom_margin(0))
        }

        let table_title_text = format!(
            "('c' to clear table | 'q' or ESC to exit) | Items: {}",
            self.data.len()
        );

        let t = Table::new(rows)
            .header(header)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(table_title_text),
            )
            .highlight_style(selected_style)
            .highlight_symbol(selected_symbol.as_str())
            .widths(&column_constraints);

        // draw
        f.render_stateful_widget(t, rects[0], &mut self.state);
    }
}

/// Return response from user input on TUI table
#[derive(Debug)]
pub enum TableEventResponse {
    InputHandled(Event),
    InputIgnored(Event),
    Terminate,
}
