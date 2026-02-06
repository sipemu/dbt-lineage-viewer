pub mod app;
pub mod event;
pub mod graph_widget;
pub mod run_status;
pub mod runner;
pub mod ui;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use crossterm::{
    event::{poll, read, DisableMouseCapture, EnableMouseCapture, Event},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::prelude::*;
use std::io;

use crate::graph::types::LineageGraph;
use crate::parser::artifacts;

use app::App;
use event::{handle_key_event, handle_mouse_event};
use ui::draw_ui;

/// Set up the terminal for TUI rendering
#[cfg(not(tarpaulin_include))]
fn setup_terminal() -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    Ok(Terminal::new(CrosstermBackend::new(stdout))?)
}

/// Restore the terminal to its original state
#[cfg(not(tarpaulin_include))]
fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        DisableMouseCapture,
        LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;
    Ok(())
}

/// Process a single terminal event. Returns true if the app should quit.
#[cfg(not(tarpaulin_include))]
fn process_event(app: &mut App, event: Event) -> bool {
    match event {
        Event::Key(key) => handle_key_event(app, key),
        Event::Mouse(mouse) => {
            handle_mouse_event(app, mouse);
            false
        }
        _ => false,
    }
}

/// Launch the interactive TUI
#[cfg(not(tarpaulin_include))]
pub fn run_tui(graph: LineageGraph, project_dir: PathBuf) -> Result<()> {
    let run_status = match artifacts::load_run_results(&project_dir)? {
        Some(results) => artifacts::build_run_status_map(&results, &graph, &project_dir),
        None => Default::default(),
    };

    let mut terminal = setup_terminal()?;
    let mut app = App::new(graph, project_dir, run_status);

    loop {
        terminal.draw(|f| draw_ui(f, &mut app))?;
        app.drain_run_messages();
        if poll(Duration::from_millis(50))? && process_event(&mut app, read()?) {
            break;
        }
    }

    restore_terminal(&mut terminal)
}
