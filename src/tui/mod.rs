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

/// Launch the interactive TUI
#[cfg(not(tarpaulin_include))]
pub fn run_tui(graph: LineageGraph, project_dir: PathBuf) -> Result<()> {
    // Load initial run status
    let run_status = match artifacts::load_run_results(&project_dir)? {
        Some(results) => artifacts::build_run_status_map(&results, &graph, &project_dir),
        None => Default::default(),
    };

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(graph, project_dir, run_status);

    // Main loop — poll-based for responsive subprocess output
    loop {
        terminal.draw(|f| draw_ui(f, &mut app))?;

        // Drain any pending dbt run messages
        app.drain_run_messages();

        // Poll with 50ms timeout so we can check subprocess output frequently
        if poll(Duration::from_millis(50))? {
            match read()? {
                Event::Key(key) => {
                    if handle_key_event(&mut app, key) {
                        break;
                    }
                }
                Event::Mouse(mouse) => {
                    handle_mouse_event(&mut app, mouse);
                }
                _ => {}
            }
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        DisableMouseCapture,
        LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;

    Ok(())
}
