use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::app::{App, AppMode, DbtRunState};
use super::runner::{detect_use_uv, DbtCommand, DbtRunRequest, SelectionScope};

const PAN_AMOUNT: i32 = 3;
const ZOOM_STEP: f64 = 0.1;

/// Handle a key event. Returns true if the app should quit.
pub fn handle_key_event(app: &mut App, key: KeyEvent) -> bool {
    match app.mode {
        AppMode::Normal => handle_normal_mode(app, key),
        AppMode::Search => handle_search_mode(app, key),
        AppMode::RunMenu => handle_run_menu_mode(app, key),
        AppMode::RunConfirm => handle_run_confirm_mode(app, key),
        AppMode::RunOutput => handle_run_output_mode(app, key),
    }
}

fn handle_normal_mode(app: &mut App, key: KeyEvent) -> bool {
    // Ctrl+C always quits
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return true;
    }

    // Shift+HJKL for camera panning
    if key.modifiers.contains(KeyModifiers::SHIFT) {
        match key.code {
            KeyCode::Char('H') => { app.viewport_x -= PAN_AMOUNT; return false; }
            KeyCode::Char('J') => { app.viewport_y += PAN_AMOUNT; return false; }
            KeyCode::Char('K') => { app.viewport_y -= PAN_AMOUNT; return false; }
            KeyCode::Char('L') => { app.viewport_x += PAN_AMOUNT; return false; }
            _ => {}
        }
    }

    match key.code {
        KeyCode::Char('q') => return true,

        // Graph navigation: hjkl or arrow keys
        KeyCode::Char('h') | KeyCode::Left => app.navigate_left(),
        KeyCode::Char('l') | KeyCode::Right => app.navigate_right(),
        KeyCode::Char('k') | KeyCode::Up => app.navigate_up(),
        KeyCode::Char('j') | KeyCode::Down => app.navigate_down(),

        // Zoom
        KeyCode::Char('+') | KeyCode::Char('=') => {
            app.zoom = (app.zoom + ZOOM_STEP).min(3.0);
        }
        KeyCode::Char('-') => {
            app.zoom = (app.zoom - ZOOM_STEP).max(0.3);
        }

        // Cycle nodes sequentially
        KeyCode::Tab => app.cycle_next_node(),
        KeyCode::BackTab => app.cycle_prev_node(),

        // Enter search mode
        KeyCode::Char('/') => {
            app.mode = AppMode::Search;
            app.search_query.clear();
        }

        // Reset view
        KeyCode::Char('r') => app.reset_view(),

        // Toggle node list panel
        KeyCode::Char('n') => {
            app.show_node_list = !app.show_node_list;
        }

        // Collapse/expand group in node list
        KeyCode::Char('c') => {
            if app.show_node_list {
                app.toggle_group_collapse();
            }
        }

        // Open run menu
        KeyCode::Char('x') => {
            if app.selected_node.is_some() && !app.is_run_in_progress() {
                app.mode = AppMode::RunMenu;
            }
        }

        // View run output
        KeyCode::Char('o') => {
            if app.has_run_output() {
                app.mode = AppMode::RunOutput;
            }
        }

        // Enter on a node
        KeyCode::Enter => {}

        _ => {}
    }

    false
}

fn handle_search_mode(app: &mut App, key: KeyEvent) -> bool {
    // Ctrl+C exits search
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        app.mode = AppMode::Normal;
        return false;
    }

    match key.code {
        KeyCode::Esc => {
            app.mode = AppMode::Normal;
        }
        KeyCode::Enter => {
            app.mode = AppMode::Normal;
        }
        KeyCode::Backspace => {
            app.search_query.pop();
            app.update_search();
        }
        KeyCode::Tab => {
            app.next_search_result();
        }
        KeyCode::Char(c) => {
            app.search_query.push(c);
            app.update_search();
        }
        _ => {}
    }

    false
}

fn handle_run_menu_mode(app: &mut App, key: KeyEvent) -> bool {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        app.mode = AppMode::Normal;
        return false;
    }

    let selected_idx = match app.selected_node {
        Some(idx) => idx,
        None => {
            app.mode = AppMode::Normal;
            return false;
        }
    };

    let model_name = app.graph[selected_idx].label.clone();
    let project_dir = app.project_dir.clone();
    let use_uv = detect_use_uv(&project_dir);

    let make_request = |command: DbtCommand, scope: SelectionScope| DbtRunRequest {
        command,
        scope,
        model_name: model_name.clone(),
        project_dir: project_dir.clone(),
        use_uv,
    };

    match key.code {
        KeyCode::Char('r') => {
            app.pending_run = Some(make_request(DbtCommand::Run, SelectionScope::Single));
            app.mode = AppMode::RunConfirm;
        }
        KeyCode::Char('u') => {
            app.pending_run = Some(make_request(DbtCommand::Run, SelectionScope::WithUpstream));
            app.mode = AppMode::RunConfirm;
        }
        KeyCode::Char('d') => {
            app.pending_run = Some(make_request(DbtCommand::Run, SelectionScope::WithDownstream));
            app.mode = AppMode::RunConfirm;
        }
        KeyCode::Char('a') => {
            app.pending_run = Some(make_request(DbtCommand::Run, SelectionScope::FullLineage));
            app.mode = AppMode::RunConfirm;
        }
        KeyCode::Char('t') => {
            app.pending_run = Some(make_request(DbtCommand::Test, SelectionScope::Single));
            app.mode = AppMode::RunConfirm;
        }
        KeyCode::Esc => {
            app.mode = AppMode::Normal;
        }
        _ => {}
    }

    false
}

fn handle_run_confirm_mode(app: &mut App, key: KeyEvent) -> bool {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        app.pending_run = None;
        app.mode = AppMode::Normal;
        return false;
    }

    match key.code {
        KeyCode::Char('y') | KeyCode::Enter => {
            app.start_dbt_run();
        }
        KeyCode::Char('n') | KeyCode::Esc => {
            app.pending_run = None;
            app.mode = AppMode::Normal;
        }
        _ => {}
    }

    false
}

fn handle_run_output_mode(app: &mut App, key: KeyEvent) -> bool {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        app.mode = AppMode::Normal;
        return false;
    }

    match key.code {
        KeyCode::Char('j') | KeyCode::Down => {
            app.run_output_scroll = app.run_output_scroll.saturating_add(1);
        }
        KeyCode::Char('k') | KeyCode::Up => {
            app.run_output_scroll = app.run_output_scroll.saturating_sub(1);
        }
        KeyCode::Char('G') => {
            // Jump to bottom
            let total_lines = match &app.run_state {
                DbtRunState::Running { output_lines, .. } => output_lines.len(),
                DbtRunState::Finished { output_lines, .. } => output_lines.len(),
                DbtRunState::Idle => 0,
            };
            app.run_output_scroll = total_lines.saturating_sub(1);
        }
        KeyCode::Esc | KeyCode::Char('q') => {
            app.mode = AppMode::Normal;
        }
        _ => {}
    }

    false
}
