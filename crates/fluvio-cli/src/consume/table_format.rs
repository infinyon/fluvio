// -----------------------------------
//  Full Table
// -----------------------------------

use crate::Result;
use tui::widgets::TableState;
use std::io::Stdout;
use tui::{
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Row, Table},
    Frame, Terminal,
};
use crossterm::event::{Event, KeyCode, MouseEventKind};

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

    /// Return number of rows in cache
    pub fn num_columns(&self) -> usize {
        self.headers.len()
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

    /// Takes in `user_input` and handles the side-effect, (except for exiting table)
    /// Returns the appropriate `TableEventResponse`
    pub fn event_handler(&mut self, user_input: Event) -> TableEventResponse {
        if let Event::Key(key) = user_input {
            match key.code {
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
                _ => TableEventResponse::InputIgnored(user_input),
            }
        } else if let Event::Mouse(event) = user_input {
            match event.kind {
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
            TableEventResponse::InputIgnored(user_input)
        }
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

        // Define the widths of the columns
        for _ in 0..self.num_columns() {
            column_constraints.push(Constraint::Percentage(equal_column_width));
        }

        let selected_symbol = format!("{} >> ", self.current_selected());

        let selected_style = Style::default().add_modifier(Modifier::REVERSED);
        let normal_style = Style::default().bg(Color::Blue);
        let header_cells = self
            .headers
            .iter()
            .map(|h| Cell::from(h.as_str()).style(Style::default().fg(Color::Red)));
        let header = Row::new(header_cells)
            .style(normal_style)
            .height(1)
            .bottom_margin(1);
        let rows = self.data.iter().map(|item| {
            let height = item
                .iter()
                .map(|content| content.chars().filter(|c| *c == '\n').count())
                .max()
                .unwrap_or(0)
                + 1;
            let cells = item.iter().map(|c| Cell::from(c.as_str()));
            Row::new(cells).height(height as u16).bottom_margin(1)
        });
        let t = Table::new(rows)
            .header(header)
            .block(Block::default().borders(Borders::ALL).title(format!(
                "('c' to clear table | 'q' or ESC to exit) | Items: {}",
                self.data.len()
            )))
            .highlight_style(selected_style)
            .highlight_symbol(selected_symbol.as_str())
            .widths(&column_constraints);
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
