use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::Rect;

use super::app::{App, AppMode, DbtRunState, DragState, NodeListEntry};
use super::graph_widget::hit_test_node;
use super::runner::{detect_use_uv, DbtCommand, DbtRunRequest, SelectionScope};

const PAN_AMOUNT: i32 = 3;
const ZOOM_STEP: f64 = 0.1;
const MENU_ITEM_COUNT: u16 = 5;

/// Check if a mouse position is on a menu item row. Returns the item index (0-4).
/// `items_y_offset` is the offset from the popup top to the first item row
/// (1 for context menu = border, 2 for run menu = border + empty line).
fn menu_item_at_pos(
    menu_area: Option<Rect>,
    items_y_offset: u16,
    column: u16,
    row: u16,
) -> Option<usize> {
    let area = menu_area?;
    let first_y = area.y + items_y_offset;
    if column >= area.x
        && column < area.x + area.width
        && row >= first_y
        && row < first_y + MENU_ITEM_COUNT
    {
        Some((row - first_y) as usize)
    } else {
        None
    }
}

/// Build a DbtRunRequest for a menu item index (0-4).
fn make_run_request_for_item(app: &App, item: usize) -> Option<DbtRunRequest> {
    let selected_idx = app.selected_node?;
    let model_name = app.graph[selected_idx].label.clone();
    let project_dir = app.project_dir.clone();
    let use_uv = detect_use_uv(&project_dir);
    let make = |command: DbtCommand, scope: SelectionScope| DbtRunRequest {
        command,
        scope,
        model_name: model_name.clone(),
        project_dir: project_dir.clone(),
        use_uv,
    };
    Some(match item {
        0 => make(DbtCommand::Run, SelectionScope::Single),
        1 => make(DbtCommand::Run, SelectionScope::WithUpstream),
        2 => make(DbtCommand::Run, SelectionScope::WithDownstream),
        3 => make(DbtCommand::Run, SelectionScope::FullLineage),
        4 => make(DbtCommand::Test, SelectionScope::Single),
        _ => return None,
    })
}

/// Clear all menu overlay state.
fn clear_menu_state(app: &mut App) {
    app.context_menu_pos = None;
    app.last_context_menu_area = None;
    app.last_run_menu_area = None;
    app.menu_hover_index = None;
}

/// Check if a mouse position hits one of the confirm dialog buttons.
/// Returns Some(true) for Execute, Some(false) for Cancel, None for neither.
/// Button layout on the last inner row (popup.y + 6):
///   "  " + " Execute (y) " + "  " + " Cancel (n) "
///   cols:  0-1  2-14          15-16  17-28  (relative to inner x)
fn confirm_button_at_pos(
    confirm_area: Option<Rect>,
    column: u16,
    row: u16,
) -> Option<bool> {
    let area = confirm_area?;
    let button_row = area.y + 6; // border(1) + 5 inner rows
    if row != button_row {
        return None;
    }
    let inner_x = area.x + 1; // skip left border
    if column >= inner_x + 2 && column <= inner_x + 14 {
        Some(true) // Execute
    } else if column >= inner_x + 17 && column <= inner_x + 28 {
        Some(false) // Cancel
    } else {
        None
    }
}

/// Handle a key event. Returns true if the app should quit.
pub fn handle_key_event(app: &mut App, key: KeyEvent) -> bool {
    match app.mode {
        AppMode::Normal => handle_normal_mode(app, key),
        AppMode::Search => handle_search_mode(app, key),
        AppMode::RunMenu => handle_run_menu_mode(app, key),
        AppMode::ContextMenu => handle_context_menu_mode(app, key),
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
                app.menu_hover_index = None;
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

fn handle_context_menu_mode(app: &mut App, key: KeyEvent) -> bool {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        app.mode = AppMode::Normal;
        app.context_menu_pos = None;
        return false;
    }

    let selected_idx = match app.selected_node {
        Some(idx) => idx,
        None => {
            app.mode = AppMode::Normal;
            app.context_menu_pos = None;
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
            app.context_menu_pos = None;
            app.mode = AppMode::RunConfirm;
        }
        KeyCode::Char('u') => {
            app.pending_run = Some(make_request(DbtCommand::Run, SelectionScope::WithUpstream));
            app.context_menu_pos = None;
            app.mode = AppMode::RunConfirm;
        }
        KeyCode::Char('d') => {
            app.pending_run = Some(make_request(DbtCommand::Run, SelectionScope::WithDownstream));
            app.context_menu_pos = None;
            app.mode = AppMode::RunConfirm;
        }
        KeyCode::Char('a') => {
            app.pending_run = Some(make_request(DbtCommand::Run, SelectionScope::FullLineage));
            app.context_menu_pos = None;
            app.mode = AppMode::RunConfirm;
        }
        KeyCode::Char('t') => {
            app.pending_run = Some(make_request(DbtCommand::Test, SelectionScope::Single));
            app.context_menu_pos = None;
            app.mode = AppMode::RunConfirm;
        }
        KeyCode::Esc => {
            app.mode = AppMode::Normal;
            app.context_menu_pos = None;
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

/// Handle a mouse event. Returns true if the app should quit (never does).
pub fn handle_mouse_event(app: &mut App, mouse: MouseEvent) -> bool {
    // Handle mouse in context menu or run menu (hover + click)
    if app.mode == AppMode::ContextMenu || app.mode == AppMode::RunMenu {
        let (menu_area, items_y_offset) = if app.mode == AppMode::ContextMenu {
            (app.last_context_menu_area, 1u16) // border only
        } else {
            (app.last_run_menu_area, 2u16) // border + empty line
        };

        match mouse.kind {
            MouseEventKind::Moved => {
                app.menu_hover_index =
                    menu_item_at_pos(menu_area, items_y_offset, mouse.column, mouse.row);
            }
            MouseEventKind::Down(MouseButton::Left) => {
                if let Some(item) =
                    menu_item_at_pos(menu_area, items_y_offset, mouse.column, mouse.row)
                {
                    if let Some(request) = make_run_request_for_item(app, item) {
                        app.pending_run = Some(request);
                        clear_menu_state(app);
                        app.mode = AppMode::RunConfirm;
                        return false;
                    }
                }
                // Click outside menu items — dismiss
                app.mode = AppMode::Normal;
                clear_menu_state(app);
            }
            MouseEventKind::Down(_) => {
                app.mode = AppMode::Normal;
                clear_menu_state(app);
            }
            _ => {}
        }
        return false;
    }

    // Handle mouse in confirm dialog (hover + click)
    if app.mode == AppMode::RunConfirm {
        match mouse.kind {
            MouseEventKind::Moved => {
                app.confirm_hover =
                    confirm_button_at_pos(app.last_confirm_area, mouse.column, mouse.row);
            }
            MouseEventKind::Down(MouseButton::Left) => {
                match confirm_button_at_pos(app.last_confirm_area, mouse.column, mouse.row) {
                    Some(true) => {
                        app.confirm_hover = None;
                        app.last_confirm_area = None;
                        app.start_dbt_run();
                    }
                    Some(false) => {
                        app.pending_run = None;
                        app.confirm_hover = None;
                        app.last_confirm_area = None;
                        app.mode = AppMode::Normal;
                    }
                    None => {
                        // Click outside buttons — dismiss
                        app.pending_run = None;
                        app.confirm_hover = None;
                        app.last_confirm_area = None;
                        app.mode = AppMode::Normal;
                    }
                }
            }
            MouseEventKind::Down(_) => {
                app.pending_run = None;
                app.confirm_hover = None;
                app.last_confirm_area = None;
                app.mode = AppMode::Normal;
            }
            _ => {}
        }
        return false;
    }

    // Only handle mouse in Normal mode
    if app.mode != AppMode::Normal {
        return false;
    }

    match mouse.kind {
        MouseEventKind::Down(MouseButton::Right) => {
            if let Some(graph_area) = app.last_graph_area {
                if mouse.column >= graph_area.x
                    && mouse.column < graph_area.x + graph_area.width
                    && mouse.row >= graph_area.y
                    && mouse.row < graph_area.y + graph_area.height
                {
                    if let Some(node_idx) = hit_test_node(app, mouse.column, mouse.row) {
                        app.selected_node = Some(node_idx);
                        app.sync_cycle_index();
                        app.sync_node_list_state();
                        app.context_menu_pos = Some((mouse.column, mouse.row));
                        app.menu_hover_index = None;
                        app.mode = AppMode::ContextMenu;
                    }
                }
            }
        }

        MouseEventKind::Down(MouseButton::Left) => {
            // Check if click is in the node list area
            if let Some(list_area) = app.last_node_list_area {
                if mouse.column >= list_area.x
                    && mouse.column < list_area.x + list_area.width
                    && mouse.row >= list_area.y
                    && mouse.row < list_area.y + list_area.height
                {
                    // Map click row to node list entry (account for border)
                    let row_in_list = mouse.row.saturating_sub(list_area.y + 1) as usize;
                    if row_in_list < app.node_list_entries.len() {
                        match app.node_list_entries[row_in_list] {
                            NodeListEntry::GroupHeader(gi) => {
                                app.toggle_group_collapse_by_index(gi);
                            }
                            NodeListEntry::Node(idx) => {
                                app.selected_node = Some(idx);
                                app.node_list_state.select(Some(row_in_list));
                                // Sync cycle index and center
                                app.center_on_selected();
                            }
                        }
                    }
                    return false;
                }
            }

            // Check if click is in the graph area
            if let Some(graph_area) = app.last_graph_area {
                if mouse.column >= graph_area.x
                    && mouse.column < graph_area.x + graph_area.width
                    && mouse.row >= graph_area.y
                    && mouse.row < graph_area.y + graph_area.height
                {
                    if let Some(node_idx) = hit_test_node(app, mouse.column, mouse.row) {
                        app.select_node_no_center(node_idx);
                    } else {
                        // Start drag for panning
                        app.drag_state = Some(DragState {
                            start_x: mouse.column,
                            start_y: mouse.row,
                            viewport_x0: app.viewport_x,
                            viewport_y0: app.viewport_y,
                        });
                    }
                }
            }
        }

        MouseEventKind::Drag(MouseButton::Left) => {
            if let Some(ref drag) = app.drag_state {
                // Natural pan direction: dragging right moves viewport left
                app.viewport_x =
                    drag.viewport_x0 - (mouse.column as i32 - drag.start_x as i32);
                app.viewport_y =
                    drag.viewport_y0 - (mouse.row as i32 - drag.start_y as i32);
            }
        }

        MouseEventKind::Up(MouseButton::Left) => {
            app.drag_state = None;
        }

        MouseEventKind::ScrollUp => {
            // Only zoom if over the graph area
            if let Some(graph_area) = app.last_graph_area {
                if mouse.column >= graph_area.x
                    && mouse.column < graph_area.x + graph_area.width
                    && mouse.row >= graph_area.y
                    && mouse.row < graph_area.y + graph_area.height
                {
                    app.zoom = (app.zoom + ZOOM_STEP).min(3.0);
                }
            }
        }

        MouseEventKind::ScrollDown => {
            if let Some(graph_area) = app.last_graph_area {
                if mouse.column >= graph_area.x
                    && mouse.column < graph_area.x + graph_area.width
                    && mouse.row >= graph_area.y
                    && mouse.row < graph_area.y + graph_area.height
                {
                    app.zoom = (app.zoom - ZOOM_STEP).max(0.3);
                }
            }
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
