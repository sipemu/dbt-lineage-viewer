use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::Rect;

use super::app::{App, AppMode, DbtRunState, DragState, FilterStatus, NodeListEntry};
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
fn confirm_button_at_pos(confirm_area: Option<Rect>, column: u16, row: u16) -> Option<bool> {
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
        AppMode::Filter => handle_filter_mode(app, key),
    }
}

/// Handle Shift+HJKL camera panning. Returns Some(false) if handled.
fn handle_shift_pan(app: &mut App, code: KeyCode) -> Option<bool> {
    match code {
        KeyCode::Char('H') => app.viewport_x -= PAN_AMOUNT,
        KeyCode::Char('J') => app.viewport_y += PAN_AMOUNT,
        KeyCode::Char('K') => app.viewport_y -= PAN_AMOUNT,
        KeyCode::Char('L') => app.viewport_x += PAN_AMOUNT,
        _ => return None,
    }
    Some(false)
}

/// Handle unmodified normal mode keys. Returns true to quit.
fn handle_normal_key(app: &mut App, code: KeyCode) -> bool {
    match code {
        KeyCode::Char('q') => return true,
        KeyCode::Char('h') | KeyCode::Left => app.navigate_left(),
        KeyCode::Char('l') | KeyCode::Right => app.navigate_right(),
        KeyCode::Char('k') | KeyCode::Up => app.navigate_up(),
        KeyCode::Char('j') | KeyCode::Down => app.navigate_down(),
        KeyCode::Char('+') | KeyCode::Char('=') => app.zoom = (app.zoom + ZOOM_STEP).min(3.0),
        KeyCode::Char('-') => app.zoom = (app.zoom - ZOOM_STEP).max(0.3),
        KeyCode::Tab => app.cycle_next_node(),
        KeyCode::BackTab => app.cycle_prev_node(),
        KeyCode::Char('/') => {
            app.mode = AppMode::Search;
            app.search_query.clear();
        }
        KeyCode::Char('r') => app.reset_view(),
        KeyCode::Char('n') => app.show_node_list = !app.show_node_list,
        KeyCode::Char('c') if app.show_node_list => app.toggle_group_collapse(),
        KeyCode::Char('x') if app.selected_node.is_some() && !app.is_run_in_progress() => {
            app.menu_hover_index = None;
            app.mode = AppMode::RunMenu;
        }
        KeyCode::Char('o') if app.has_run_output() => app.mode = AppMode::RunOutput,
        KeyCode::Char('f') => app.mode = AppMode::Filter,
        KeyCode::Char('p') => app.toggle_path_highlight(),
        KeyCode::Char('C') => app.toggle_column_lineage(),
        _ => {}
    }
    false
}

fn handle_normal_mode(app: &mut App, key: KeyEvent) -> bool {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return true;
    }
    if key.modifiers.contains(KeyModifiers::SHIFT) {
        if let Some(result) = handle_shift_pan(app, key.code) {
            return result;
        }
    }
    handle_normal_key(app, key.code)
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
            app.pending_run = Some(make_request(
                DbtCommand::Run,
                SelectionScope::WithDownstream,
            ));
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
            app.pending_run = Some(make_request(
                DbtCommand::Run,
                SelectionScope::WithDownstream,
            ));
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

/// Check if a mouse position is within a Rect area
fn is_within(area: Rect, column: u16, row: u16) -> bool {
    column >= area.x && column < area.x + area.width && row >= area.y && row < area.y + area.height
}

/// Handle mouse in ContextMenu or RunMenu mode
fn handle_mouse_menu(app: &mut App, mouse: MouseEvent) {
    let (menu_area, items_y_offset) = if app.mode == AppMode::ContextMenu {
        (app.last_context_menu_area, 1u16)
    } else {
        (app.last_run_menu_area, 2u16)
    };

    match mouse.kind {
        MouseEventKind::Moved => {
            app.menu_hover_index =
                menu_item_at_pos(menu_area, items_y_offset, mouse.column, mouse.row);
        }
        MouseEventKind::Down(MouseButton::Left) => {
            if let Some(item) = menu_item_at_pos(menu_area, items_y_offset, mouse.column, mouse.row)
            {
                if let Some(request) = make_run_request_for_item(app, item) {
                    app.pending_run = Some(request);
                    clear_menu_state(app);
                    app.mode = AppMode::RunConfirm;
                    return;
                }
            }
            app.mode = AppMode::Normal;
            clear_menu_state(app);
        }
        MouseEventKind::Down(_) => {
            app.mode = AppMode::Normal;
            clear_menu_state(app);
        }
        _ => {}
    }
}

/// Clear confirm dialog state and return to Normal mode
fn dismiss_confirm(app: &mut App) {
    app.pending_run = None;
    app.confirm_hover = None;
    app.last_confirm_area = None;
    app.mode = AppMode::Normal;
}

/// Handle mouse in RunConfirm mode
fn handle_mouse_confirm(app: &mut App, mouse: MouseEvent) {
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
                Some(false) | None => dismiss_confirm(app),
            }
        }
        MouseEventKind::Down(_) => dismiss_confirm(app),
        _ => {}
    }
}

/// Handle left-click on the node list panel
fn handle_node_list_click(app: &mut App, column: u16, row: u16) -> bool {
    let Some(list_area) = app.last_node_list_area else {
        return false;
    };
    if !is_within(list_area, column, row) {
        return false;
    }
    let row_in_list = row.saturating_sub(list_area.y + 1) as usize;
    if row_in_list < app.node_list_entries.len() {
        match app.node_list_entries[row_in_list] {
            NodeListEntry::GroupHeader(gi) => app.toggle_group_collapse_by_index(gi),
            NodeListEntry::Node(idx) => {
                app.selected_node = Some(idx);
                app.node_list_state.select(Some(row_in_list));
                app.center_on_selected();
            }
        }
    }
    true // click was consumed
}

/// Handle left-click on the graph area (node select or drag start)
fn handle_graph_left_click(app: &mut App, column: u16, row: u16) {
    let Some(graph_area) = app.last_graph_area else {
        return;
    };
    if !is_within(graph_area, column, row) {
        return;
    }
    if let Some(node_idx) = hit_test_node(app, column, row) {
        app.select_node_no_center(node_idx);
    } else {
        app.drag_state = Some(DragState {
            start_x: column,
            start_y: row,
            viewport_x0: app.viewport_x,
            viewport_y0: app.viewport_y,
        });
    }
}

/// Handle right-click in the graph area (open context menu)
fn handle_graph_right_click(app: &mut App, column: u16, row: u16) {
    let Some(graph_area) = app.last_graph_area else {
        return;
    };
    if !is_within(graph_area, column, row) {
        return;
    }
    if let Some(node_idx) = hit_test_node(app, column, row) {
        app.selected_node = Some(node_idx);
        app.sync_cycle_index();
        app.sync_node_list_state();
        app.context_menu_pos = Some((column, row));
        app.menu_hover_index = None;
        app.mode = AppMode::ContextMenu;
    }
}

/// Handle scroll zoom on the graph area
fn handle_graph_scroll(app: &mut App, column: u16, row: u16, zoom_in: bool) {
    let Some(graph_area) = app.last_graph_area else {
        return;
    };
    if !is_within(graph_area, column, row) {
        return;
    }
    if zoom_in {
        app.zoom = (app.zoom + ZOOM_STEP).min(3.0);
    } else {
        app.zoom = (app.zoom - ZOOM_STEP).max(0.3);
    }
}

/// Handle a mouse event. Returns true if the app should quit (never does).
pub fn handle_mouse_event(app: &mut App, mouse: MouseEvent) -> bool {
    match app.mode {
        AppMode::ContextMenu | AppMode::RunMenu => handle_mouse_menu(app, mouse),
        AppMode::RunConfirm => handle_mouse_confirm(app, mouse),
        AppMode::Normal | AppMode::Filter => handle_mouse_normal(app, mouse),
        _ => {}
    }
    false
}

/// Handle mouse events in Normal mode
fn handle_mouse_normal(app: &mut App, mouse: MouseEvent) {
    match mouse.kind {
        MouseEventKind::Down(MouseButton::Right) => {
            handle_graph_right_click(app, mouse.column, mouse.row);
        }
        MouseEventKind::Down(MouseButton::Left) => {
            if !handle_node_list_click(app, mouse.column, mouse.row) {
                handle_graph_left_click(app, mouse.column, mouse.row);
            }
        }
        MouseEventKind::Drag(MouseButton::Left) => {
            if let Some(ref drag) = app.drag_state {
                app.viewport_x = drag.viewport_x0 - (mouse.column as i32 - drag.start_x as i32);
                app.viewport_y = drag.viewport_y0 - (mouse.row as i32 - drag.start_y as i32);
            }
        }
        MouseEventKind::Up(MouseButton::Left) => {
            app.drag_state = None;
        }
        MouseEventKind::ScrollUp => {
            handle_graph_scroll(app, mouse.column, mouse.row, true);
        }
        MouseEventKind::ScrollDown => {
            handle_graph_scroll(app, mouse.column, mouse.row, false);
        }
        _ => {}
    }
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

fn handle_filter_mode(app: &mut App, key: KeyEvent) -> bool {
    use crate::graph::types::NodeType;

    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        app.mode = AppMode::Normal;
        return false;
    }

    match key.code {
        KeyCode::Char('m') => app.toggle_filter_node_type(NodeType::Model),
        KeyCode::Char('s') => app.toggle_filter_node_type(NodeType::Source),
        KeyCode::Char('e') => app.toggle_filter_node_type(NodeType::Exposure),
        KeyCode::Char('t') => app.toggle_filter_node_type(NodeType::Test),
        KeyCode::Char('d') => app.toggle_filter_node_type(NodeType::Seed),
        KeyCode::Char('1') => {
            app.filter_status = Some(FilterStatus::Errored);
        }
        KeyCode::Char('2') => {
            app.filter_status = Some(FilterStatus::Success);
        }
        KeyCode::Char('3') => {
            app.filter_status = Some(FilterStatus::NeverRun);
        }
        KeyCode::Char('0') => {
            app.filter_status = None;
        }
        KeyCode::Esc => {
            app.mode = AppMode::Normal;
        }
        _ => {}
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::*;
    use crate::parser::artifacts::RunStatusMap;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn make_test_graph() -> LineageGraph {
        let mut graph = LineageGraph::new();
        let src = graph.add_node(NodeData {
            unique_id: "source.raw.orders".into(),
            label: "raw.orders".into(),
            node_type: NodeType::Source,
            file_path: Some(PathBuf::from("models/schema.yml")),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let stg = graph.add_node(NodeData {
            unique_id: "model.stg_orders".into(),
            label: "stg_orders".into(),
            node_type: NodeType::Model,
            file_path: Some(PathBuf::from("models/staging/stg_orders.sql")),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let mart = graph.add_node(NodeData {
            unique_id: "model.orders".into(),
            label: "orders".into(),
            node_type: NodeType::Model,
            file_path: Some(PathBuf::from("models/marts/orders.sql")),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let exp = graph.add_node(NodeData {
            unique_id: "exposure.dashboard".into(),
            label: "dashboard".into(),
            node_type: NodeType::Exposure,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        graph.add_edge(
            src,
            stg,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        graph.add_edge(
            stg,
            mart,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        graph.add_edge(
            mart,
            exp,
            EdgeData {
                edge_type: EdgeType::Exposure,
            },
        );
        graph
    }

    fn test_app() -> App {
        let run_status: RunStatusMap = HashMap::new();
        App::new(make_test_graph(), PathBuf::from("/tmp"), run_status)
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_shift(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::SHIFT)
    }

    fn key_ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    // ─── Normal mode tests ───

    #[test]
    fn test_normal_q_quits() {
        let mut app = test_app();
        assert!(handle_key_event(&mut app, key(KeyCode::Char('q'))));
    }

    #[test]
    fn test_normal_ctrl_c_quits() {
        let mut app = test_app();
        assert!(handle_key_event(&mut app, key_ctrl('c')));
    }

    #[test]
    fn test_normal_hjkl_navigate() {
        let mut app = test_app();
        let initial = app.selected_node;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('l'))));
        // May or may not change depending on graph structure
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('h'))));
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('j'))));
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('k'))));
        // Arrow keys
        assert!(!handle_key_event(&mut app, key(KeyCode::Right)));
        assert!(!handle_key_event(&mut app, key(KeyCode::Left)));
        assert!(!handle_key_event(&mut app, key(KeyCode::Down)));
        assert!(!handle_key_event(&mut app, key(KeyCode::Up)));
        let _ = initial; // suppress unused
    }

    #[test]
    fn test_normal_shift_hjkl_pan() {
        let mut app = test_app();
        let vx = app.viewport_x;
        assert!(!handle_key_event(&mut app, key_shift(KeyCode::Char('H'))));
        assert!(app.viewport_x < vx);
        let vy = app.viewport_y;
        assert!(!handle_key_event(&mut app, key_shift(KeyCode::Char('J'))));
        assert!(app.viewport_y > vy);
        let vy2 = app.viewport_y;
        assert!(!handle_key_event(&mut app, key_shift(KeyCode::Char('K'))));
        assert!(app.viewport_y < vy2);
        let vx2 = app.viewport_x;
        assert!(!handle_key_event(&mut app, key_shift(KeyCode::Char('L'))));
        assert!(app.viewport_x > vx2);
    }

    #[test]
    fn test_normal_zoom() {
        let mut app = test_app();
        let z = app.zoom;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('+'))));
        assert!(app.zoom > z);
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('-'))));
        assert!((app.zoom - z).abs() < 0.001);
        // '=' also zooms in
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('='))));
        assert!(app.zoom > z);
    }

    #[test]
    fn test_normal_tab_cycle() {
        let mut app = test_app();
        let first = app.selected_node;
        assert!(!handle_key_event(&mut app, key(KeyCode::Tab)));
        assert_ne!(app.selected_node, first);
        assert!(!handle_key_event(&mut app, key(KeyCode::BackTab)));
        assert_eq!(app.selected_node, first);
    }

    #[test]
    fn test_normal_slash_enters_search() {
        let mut app = test_app();
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('/'))));
        assert_eq!(app.mode, AppMode::Search);
        assert!(app.search_query.is_empty());
    }

    #[test]
    fn test_normal_r_reset() {
        let mut app = test_app();
        app.viewport_x = 50;
        app.viewport_y = 30;
        app.zoom = 2.0;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('r'))));
        assert_eq!(app.viewport_x, 0);
        assert_eq!(app.viewport_y, 0);
        assert_eq!(app.zoom, 1.0);
    }

    #[test]
    fn test_normal_n_toggle_node_list() {
        let mut app = test_app();
        assert!(!app.show_node_list);
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('n'))));
        assert!(app.show_node_list);
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('n'))));
        assert!(!app.show_node_list);
    }

    #[test]
    fn test_normal_c_collapse() {
        let mut app = test_app();
        app.show_node_list = true;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('c'))));
        // Should toggle collapse for selected node's group
    }

    #[test]
    fn test_normal_c_no_node_list() {
        let mut app = test_app();
        app.show_node_list = false;
        // 'c' with no node list should be a no-op
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('c'))));
    }

    #[test]
    fn test_normal_x_opens_run_menu() {
        let mut app = test_app();
        assert!(app.selected_node.is_some());
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('x'))));
        assert_eq!(app.mode, AppMode::RunMenu);
    }

    #[test]
    fn test_normal_x_no_selection() {
        let mut app = test_app();
        app.selected_node = None;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('x'))));
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_normal_o_view_output() {
        let mut app = test_app();
        app.run_state = DbtRunState::Finished {
            output_lines: vec!["done".into()],
            success: true,
        };
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('o'))));
        assert_eq!(app.mode, AppMode::RunOutput);
    }

    #[test]
    fn test_normal_o_no_output() {
        let mut app = test_app();
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('o'))));
        assert_eq!(app.mode, AppMode::Normal);
    }

    // ─── Search mode tests ───

    #[test]
    fn test_search_char_input() {
        let mut app = test_app();
        app.mode = AppMode::Search;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('o'))));
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('r'))));
        assert_eq!(app.search_query, "or");
    }

    #[test]
    fn test_search_backspace() {
        let mut app = test_app();
        app.mode = AppMode::Search;
        app.search_query = "ord".into();
        assert!(!handle_key_event(&mut app, key(KeyCode::Backspace)));
        assert_eq!(app.search_query, "or");
    }

    #[test]
    fn test_search_tab_next() {
        let mut app = test_app();
        app.mode = AppMode::Search;
        app.search_query = "orders".into();
        app.update_search();
        let first = app.selected_node;
        assert!(!handle_key_event(&mut app, key(KeyCode::Tab)));
        if app.search_results.len() > 1 {
            assert_ne!(app.selected_node, first);
        }
    }

    #[test]
    fn test_search_esc_exits() {
        let mut app = test_app();
        app.mode = AppMode::Search;
        assert!(!handle_key_event(&mut app, key(KeyCode::Esc)));
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_search_enter_exits() {
        let mut app = test_app();
        app.mode = AppMode::Search;
        assert!(!handle_key_event(&mut app, key(KeyCode::Enter)));
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_search_ctrl_c_exits() {
        let mut app = test_app();
        app.mode = AppMode::Search;
        assert!(!handle_key_event(&mut app, key_ctrl('c')));
        assert_eq!(app.mode, AppMode::Normal);
    }

    // ─── RunMenu mode tests ───

    #[test]
    fn test_run_menu_r() {
        let mut app = test_app();
        app.mode = AppMode::RunMenu;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('r'))));
        assert_eq!(app.mode, AppMode::RunConfirm);
        assert!(app.pending_run.is_some());
    }

    #[test]
    fn test_run_menu_u() {
        let mut app = test_app();
        app.mode = AppMode::RunMenu;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('u'))));
        assert_eq!(app.mode, AppMode::RunConfirm);
        assert!(app.pending_run.is_some());
        assert_eq!(
            app.pending_run.as_ref().unwrap().scope,
            SelectionScope::WithUpstream
        );
    }

    #[test]
    fn test_run_menu_d() {
        let mut app = test_app();
        app.mode = AppMode::RunMenu;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('d'))));
        assert_eq!(app.mode, AppMode::RunConfirm);
        assert_eq!(
            app.pending_run.as_ref().unwrap().scope,
            SelectionScope::WithDownstream
        );
    }

    #[test]
    fn test_run_menu_a() {
        let mut app = test_app();
        app.mode = AppMode::RunMenu;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('a'))));
        assert_eq!(app.mode, AppMode::RunConfirm);
        assert_eq!(
            app.pending_run.as_ref().unwrap().scope,
            SelectionScope::FullLineage
        );
    }

    #[test]
    fn test_run_menu_t() {
        let mut app = test_app();
        app.mode = AppMode::RunMenu;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('t'))));
        assert_eq!(app.mode, AppMode::RunConfirm);
        assert_eq!(app.pending_run.as_ref().unwrap().command, DbtCommand::Test);
    }

    #[test]
    fn test_run_menu_esc() {
        let mut app = test_app();
        app.mode = AppMode::RunMenu;
        assert!(!handle_key_event(&mut app, key(KeyCode::Esc)));
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_run_menu_no_selection() {
        let mut app = test_app();
        app.mode = AppMode::RunMenu;
        app.selected_node = None;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('r'))));
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_run_menu_ctrl_c() {
        let mut app = test_app();
        app.mode = AppMode::RunMenu;
        assert!(!handle_key_event(&mut app, key_ctrl('c')));
        assert_eq!(app.mode, AppMode::Normal);
    }

    // ─── ContextMenu mode tests ───

    #[test]
    fn test_context_menu_r() {
        let mut app = test_app();
        app.mode = AppMode::ContextMenu;
        app.context_menu_pos = Some((10, 10));
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('r'))));
        assert_eq!(app.mode, AppMode::RunConfirm);
        assert!(app.context_menu_pos.is_none());
    }

    #[test]
    fn test_context_menu_esc() {
        let mut app = test_app();
        app.mode = AppMode::ContextMenu;
        app.context_menu_pos = Some((10, 10));
        assert!(!handle_key_event(&mut app, key(KeyCode::Esc)));
        assert_eq!(app.mode, AppMode::Normal);
        assert!(app.context_menu_pos.is_none());
    }

    #[test]
    fn test_context_menu_no_selection() {
        let mut app = test_app();
        app.mode = AppMode::ContextMenu;
        app.selected_node = None;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('r'))));
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_context_menu_ctrl_c() {
        let mut app = test_app();
        app.mode = AppMode::ContextMenu;
        app.context_menu_pos = Some((10, 10));
        assert!(!handle_key_event(&mut app, key_ctrl('c')));
        assert_eq!(app.mode, AppMode::Normal);
        assert!(app.context_menu_pos.is_none());
    }

    #[test]
    fn test_context_menu_u() {
        let mut app = test_app();
        app.mode = AppMode::ContextMenu;
        app.context_menu_pos = Some((10, 10));
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('u'))));
        assert_eq!(app.mode, AppMode::RunConfirm);
        assert!(app.context_menu_pos.is_none());
    }

    #[test]
    fn test_context_menu_d() {
        let mut app = test_app();
        app.mode = AppMode::ContextMenu;
        app.context_menu_pos = Some((10, 10));
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('d'))));
        assert_eq!(app.mode, AppMode::RunConfirm);
    }

    #[test]
    fn test_context_menu_a() {
        let mut app = test_app();
        app.mode = AppMode::ContextMenu;
        app.context_menu_pos = Some((10, 10));
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('a'))));
        assert_eq!(app.mode, AppMode::RunConfirm);
    }

    #[test]
    fn test_context_menu_t() {
        let mut app = test_app();
        app.mode = AppMode::ContextMenu;
        app.context_menu_pos = Some((10, 10));
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('t'))));
        assert_eq!(app.mode, AppMode::RunConfirm);
        assert_eq!(app.pending_run.as_ref().unwrap().command, DbtCommand::Test);
    }

    // ─── RunConfirm mode tests ───

    #[test]
    fn test_run_confirm_n_cancels() {
        let mut app = test_app();
        app.mode = AppMode::RunConfirm;
        app.pending_run = Some(DbtRunRequest {
            command: DbtCommand::Run,
            scope: SelectionScope::Single,
            model_name: "orders".into(),
            project_dir: PathBuf::from("/tmp"),
            use_uv: false,
        });
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('n'))));
        assert_eq!(app.mode, AppMode::Normal);
        assert!(app.pending_run.is_none());
    }

    #[test]
    fn test_run_confirm_esc_cancels() {
        let mut app = test_app();
        app.mode = AppMode::RunConfirm;
        app.pending_run = Some(DbtRunRequest {
            command: DbtCommand::Run,
            scope: SelectionScope::Single,
            model_name: "orders".into(),
            project_dir: PathBuf::from("/tmp"),
            use_uv: false,
        });
        assert!(!handle_key_event(&mut app, key(KeyCode::Esc)));
        assert_eq!(app.mode, AppMode::Normal);
        assert!(app.pending_run.is_none());
    }

    #[test]
    fn test_run_confirm_ctrl_c_cancels() {
        let mut app = test_app();
        app.mode = AppMode::RunConfirm;
        app.pending_run = Some(DbtRunRequest {
            command: DbtCommand::Run,
            scope: SelectionScope::Single,
            model_name: "orders".into(),
            project_dir: PathBuf::from("/tmp"),
            use_uv: false,
        });
        assert!(!handle_key_event(&mut app, key_ctrl('c')));
        assert_eq!(app.mode, AppMode::Normal);
        assert!(app.pending_run.is_none());
    }

    // ─── RunOutput mode tests ───

    #[test]
    fn test_run_output_scroll_down() {
        let mut app = test_app();
        app.mode = AppMode::RunOutput;
        app.run_state = DbtRunState::Finished {
            output_lines: vec!["a".into(), "b".into(), "c".into()],
            success: true,
        };
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('j'))));
        assert_eq!(app.run_output_scroll, 1);
        assert!(!handle_key_event(&mut app, key(KeyCode::Down)));
        assert_eq!(app.run_output_scroll, 2);
    }

    #[test]
    fn test_run_output_scroll_up() {
        let mut app = test_app();
        app.mode = AppMode::RunOutput;
        app.run_output_scroll = 3;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('k'))));
        assert_eq!(app.run_output_scroll, 2);
        assert!(!handle_key_event(&mut app, key(KeyCode::Up)));
        assert_eq!(app.run_output_scroll, 1);
    }

    #[test]
    fn test_run_output_jump_bottom() {
        let mut app = test_app();
        app.mode = AppMode::RunOutput;
        app.run_state = DbtRunState::Finished {
            output_lines: vec!["a".into(), "b".into(), "c".into(), "d".into()],
            success: true,
        };
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('G'))));
        assert_eq!(app.run_output_scroll, 3);
    }

    #[test]
    fn test_run_output_esc_exits() {
        let mut app = test_app();
        app.mode = AppMode::RunOutput;
        assert!(!handle_key_event(&mut app, key(KeyCode::Esc)));
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_run_output_q_exits() {
        let mut app = test_app();
        app.mode = AppMode::RunOutput;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('q'))));
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_run_output_ctrl_c_exits() {
        let mut app = test_app();
        app.mode = AppMode::RunOutput;
        assert!(!handle_key_event(&mut app, key_ctrl('c')));
        assert_eq!(app.mode, AppMode::Normal);
    }

    // ─── Mouse tests ───

    #[test]
    fn test_mouse_scroll_zoom() {
        let mut app = test_app();
        app.last_graph_area = Some(Rect::new(0, 0, 80, 24));

        let scroll_up = MouseEvent {
            kind: MouseEventKind::ScrollUp,
            column: 10,
            row: 10,
            modifiers: KeyModifiers::NONE,
        };
        let z = app.zoom;
        handle_mouse_event(&mut app, scroll_up);
        assert!(app.zoom > z);

        let scroll_down = MouseEvent {
            kind: MouseEventKind::ScrollDown,
            column: 10,
            row: 10,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, scroll_down);
        assert!((app.zoom - z).abs() < 0.001);
    }

    #[test]
    fn test_mouse_scroll_outside_graph() {
        let mut app = test_app();
        app.last_graph_area = Some(Rect::new(0, 0, 40, 20));

        let scroll = MouseEvent {
            kind: MouseEventKind::ScrollUp,
            column: 60, // outside
            row: 10,
            modifiers: KeyModifiers::NONE,
        };
        let z = app.zoom;
        handle_mouse_event(&mut app, scroll);
        assert_eq!(app.zoom, z);
    }

    #[test]
    fn test_mouse_drag_pan() {
        let mut app = test_app();
        app.last_graph_area = Some(Rect::new(0, 0, 80, 24));

        // Left click on empty space to start drag
        let down = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 70, // likely empty space
            row: 20,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, down);

        if app.drag_state.is_some() {
            // Drag
            let drag = MouseEvent {
                kind: MouseEventKind::Drag(MouseButton::Left),
                column: 75,
                row: 22,
                modifiers: KeyModifiers::NONE,
            };
            handle_mouse_event(&mut app, drag);

            // Release
            let up = MouseEvent {
                kind: MouseEventKind::Up(MouseButton::Left),
                column: 75,
                row: 22,
                modifiers: KeyModifiers::NONE,
            };
            handle_mouse_event(&mut app, up);
            assert!(app.drag_state.is_none());
        }
    }

    #[test]
    fn test_mouse_not_in_normal_mode() {
        let mut app = test_app();
        app.mode = AppMode::Search;
        let scroll = MouseEvent {
            kind: MouseEventKind::ScrollUp,
            column: 10,
            row: 10,
            modifiers: KeyModifiers::NONE,
        };
        let z = app.zoom;
        handle_mouse_event(&mut app, scroll);
        assert_eq!(app.zoom, z); // Should not zoom in search mode
    }

    #[test]
    fn test_menu_item_at_pos() {
        let area = Some(Rect::new(10, 5, 30, 10));
        // items_y_offset = 2 for run menu, first item at y=7
        assert_eq!(menu_item_at_pos(area, 2, 15, 7), Some(0));
        assert_eq!(menu_item_at_pos(area, 2, 15, 8), Some(1));
        assert_eq!(menu_item_at_pos(area, 2, 15, 11), Some(4));
        assert_eq!(menu_item_at_pos(area, 2, 15, 12), None); // past items
        assert_eq!(menu_item_at_pos(area, 2, 5, 7), None); // outside x
        assert_eq!(menu_item_at_pos(None, 2, 15, 7), None);
    }

    #[test]
    fn test_confirm_button_at_pos() {
        let area = Some(Rect::new(10, 5, 60, 8));
        // Button row is at y = 5 + 6 = 11
        // Execute: inner_x+2 to inner_x+14 = 13..25
        assert_eq!(confirm_button_at_pos(area, 13, 11), Some(true));
        // Cancel: inner_x+17 to inner_x+28 = 28..39
        assert_eq!(confirm_button_at_pos(area, 28, 11), Some(false));
        // Between buttons
        assert_eq!(confirm_button_at_pos(area, 26, 11), None);
        // Wrong row
        assert_eq!(confirm_button_at_pos(area, 13, 10), None);
        // No area
        assert_eq!(confirm_button_at_pos(None, 13, 11), None);
    }

    #[test]
    fn test_context_menu_mouse_dismiss() {
        let mut app = test_app();
        app.mode = AppMode::ContextMenu;
        app.last_context_menu_area = Some(Rect::new(10, 10, 30, 10));

        // Click outside menu
        let click = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 5,
            row: 5,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, click);
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_run_menu_mouse_hover() {
        let mut app = test_app();
        app.mode = AppMode::RunMenu;
        app.last_run_menu_area = Some(Rect::new(10, 5, 30, 14));

        let moved = MouseEvent {
            kind: MouseEventKind::Moved,
            column: 15,
            row: 7, // items_y_offset=2 → first item at y=7
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, moved);
        assert_eq!(app.menu_hover_index, Some(0));
    }

    #[test]
    fn test_confirm_dialog_mouse_cancel() {
        let mut app = test_app();
        app.mode = AppMode::RunConfirm;
        app.pending_run = Some(DbtRunRequest {
            command: DbtCommand::Run,
            scope: SelectionScope::Single,
            model_name: "orders".into(),
            project_dir: PathBuf::from("/tmp"),
            use_uv: false,
        });
        app.last_confirm_area = Some(Rect::new(10, 5, 60, 8));

        // Click Cancel button: inner_x+17=28, row=11
        let click = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 28,
            row: 11,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, click);
        assert_eq!(app.mode, AppMode::Normal);
        assert!(app.pending_run.is_none());
    }

    #[test]
    fn test_confirm_dialog_mouse_dismiss() {
        let mut app = test_app();
        app.mode = AppMode::RunConfirm;
        app.pending_run = Some(DbtRunRequest {
            command: DbtCommand::Run,
            scope: SelectionScope::Single,
            model_name: "orders".into(),
            project_dir: PathBuf::from("/tmp"),
            use_uv: false,
        });
        app.last_confirm_area = Some(Rect::new(10, 5, 60, 8));

        // Click outside buttons
        let click = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 5,
            row: 3,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, click);
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_confirm_dialog_mouse_hover() {
        let mut app = test_app();
        app.mode = AppMode::RunConfirm;
        app.last_confirm_area = Some(Rect::new(10, 5, 60, 8));

        let moved = MouseEvent {
            kind: MouseEventKind::Moved,
            column: 13, // Execute button
            row: 11,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, moved);
        assert_eq!(app.confirm_hover, Some(true));
    }

    #[test]
    fn test_confirm_dialog_right_click_dismisses() {
        let mut app = test_app();
        app.mode = AppMode::RunConfirm;
        app.pending_run = Some(DbtRunRequest {
            command: DbtCommand::Run,
            scope: SelectionScope::Single,
            model_name: "orders".into(),
            project_dir: PathBuf::from("/tmp"),
            use_uv: false,
        });
        app.last_confirm_area = Some(Rect::new(10, 5, 60, 8));

        let click = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Right),
            column: 15,
            row: 8,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, click);
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_make_run_request_for_item() {
        let app = test_app();
        // Item 0 = run single
        let req = make_run_request_for_item(&app, 0);
        assert!(req.is_some());
        let req = req.unwrap();
        assert_eq!(req.command, DbtCommand::Run);
        assert_eq!(req.scope, SelectionScope::Single);

        // Item 1 = run +upstream
        let req = make_run_request_for_item(&app, 1).unwrap();
        assert_eq!(req.scope, SelectionScope::WithUpstream);

        // Item 2 = run downstream+
        let req = make_run_request_for_item(&app, 2).unwrap();
        assert_eq!(req.scope, SelectionScope::WithDownstream);

        // Item 3 = run +all+
        let req = make_run_request_for_item(&app, 3).unwrap();
        assert_eq!(req.scope, SelectionScope::FullLineage);

        // Item 4 = test
        let req = make_run_request_for_item(&app, 4).unwrap();
        assert_eq!(req.command, DbtCommand::Test);

        // Item 5 = out of range
        assert!(make_run_request_for_item(&app, 5).is_none());
    }

    #[test]
    fn test_make_run_request_no_selection() {
        let mut app = test_app();
        app.selected_node = None;
        assert!(make_run_request_for_item(&app, 0).is_none());
    }

    #[test]
    fn test_clear_menu_state() {
        let mut app = test_app();
        app.context_menu_pos = Some((10, 10));
        app.last_context_menu_area = Some(Rect::new(0, 0, 30, 10));
        app.last_run_menu_area = Some(Rect::new(0, 0, 30, 10));
        app.menu_hover_index = Some(2);
        clear_menu_state(&mut app);
        assert!(app.context_menu_pos.is_none());
        assert!(app.last_context_menu_area.is_none());
        assert!(app.last_run_menu_area.is_none());
        assert!(app.menu_hover_index.is_none());
    }

    #[test]
    fn test_run_menu_mouse_click_item() {
        let mut app = test_app();
        app.mode = AppMode::RunMenu;
        app.last_run_menu_area = Some(Rect::new(10, 5, 30, 14));

        // Click on first item (y=7, items_y_offset=2)
        let click = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 15,
            row: 7,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, click);
        assert_eq!(app.mode, AppMode::RunConfirm);
        assert!(app.pending_run.is_some());
    }

    #[test]
    fn test_context_menu_mouse_click_item() {
        let mut app = test_app();
        app.mode = AppMode::ContextMenu;
        app.last_context_menu_area = Some(Rect::new(10, 5, 30, 10));

        // Click on first item (y=6, items_y_offset=1 for context menu)
        let click = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 15,
            row: 6,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, click);
        assert_eq!(app.mode, AppMode::RunConfirm);
    }

    #[test]
    fn test_context_menu_right_click_dismiss() {
        let mut app = test_app();
        app.mode = AppMode::ContextMenu;
        app.last_context_menu_area = Some(Rect::new(10, 5, 30, 10));

        let click = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Right),
            column: 5,
            row: 5,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, click);
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_node_list_click_node() {
        use ratatui::layout::Rect;
        let mut app = test_app();
        app.show_node_list = true;
        app.last_graph_area = Some(Rect::new(20, 0, 60, 24));
        app.last_node_list_area = Some(Rect::new(0, 0, 20, 24));

        // Click on a node entry (row 2 = first node after group header at row 0+1=1 border)
        let click = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 5,
            row: 2, // border(1) + second entry
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, click);
        // Should have selected a node (or toggled group)
    }

    #[test]
    fn test_confirm_execute_click() {
        let mut app = test_app();
        app.mode = AppMode::RunConfirm;
        app.pending_run = Some(DbtRunRequest {
            command: DbtCommand::Run,
            scope: SelectionScope::Single,
            model_name: "orders".into(),
            project_dir: PathBuf::from("/tmp"),
            use_uv: false,
        });
        app.last_confirm_area = Some(Rect::new(10, 5, 60, 8));

        // Click Execute button: inner_x+2=13, row=11
        let click = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 13,
            row: 11,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, click);
        // Should have started the run (mode goes to RunOutput)
        assert_eq!(app.mode, AppMode::RunOutput);
    }

    #[test]
    fn test_run_output_jump_bottom_running() {
        let mut app = test_app();
        app.mode = AppMode::RunOutput;
        let (_tx, rx) = std::sync::mpsc::channel::<super::super::runner::DbtRunMessage>();
        app.run_state = DbtRunState::Running {
            receiver: rx,
            output_lines: vec!["a".into(), "b".into(), "c".into()],
        };
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('G'))));
        assert_eq!(app.run_output_scroll, 2);
    }

    #[test]
    fn test_run_output_jump_bottom_idle() {
        let mut app = test_app();
        app.mode = AppMode::RunOutput;
        // Idle state has 0 lines
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('G'))));
        assert_eq!(app.run_output_scroll, 0);
    }

    #[test]
    fn test_right_click_outside_graph_area() {
        let mut app = test_app();
        app.last_graph_area = Some(Rect::new(10, 5, 60, 20));

        // Right-click outside graph area
        let mouse = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Right),
            column: 5, // outside x
            row: 10,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, mouse);
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_left_click_node_in_graph() {
        use crate::tui::graph_widget::GraphWidget;
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;

        let mut app = test_app();
        // Render graph to set last_graph_area and positions
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| {
                let area = f.area();
                app.last_graph_area = Some(area);
                f.render_widget(GraphWidget::new(&app), area);
            })
            .unwrap();

        // Left-click on a node area (first node at ~(5,1))
        let click = MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 5,
            row: 1,
            modifiers: KeyModifiers::NONE,
        };
        handle_mouse_event(&mut app, click);
        // Should select the node without starting a drag
        assert!(app.drag_state.is_none());
    }

    // ─── Filter mode tests ───

    #[test]
    fn test_normal_f_enters_filter() {
        let mut app = test_app();
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('f'))));
        assert_eq!(app.mode, AppMode::Filter);
    }

    #[test]
    fn test_filter_esc_exits() {
        let mut app = test_app();
        app.mode = AppMode::Filter;
        assert!(!handle_key_event(&mut app, key(KeyCode::Esc)));
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_filter_ctrl_c_exits() {
        let mut app = test_app();
        app.mode = AppMode::Filter;
        assert!(!handle_key_event(&mut app, key_ctrl('c')));
        assert_eq!(app.mode, AppMode::Normal);
    }

    #[test]
    fn test_filter_toggle_models() {
        let mut app = test_app();
        app.mode = AppMode::Filter;
        assert!(app
            .filter_node_types
            .contains(&crate::graph::types::NodeType::Model));
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('m'))));
        assert!(!app
            .filter_node_types
            .contains(&crate::graph::types::NodeType::Model));
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('m'))));
        assert!(app
            .filter_node_types
            .contains(&crate::graph::types::NodeType::Model));
    }

    #[test]
    fn test_filter_toggle_sources() {
        let mut app = test_app();
        app.mode = AppMode::Filter;
        assert!(app
            .filter_node_types
            .contains(&crate::graph::types::NodeType::Source));
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('s'))));
        assert!(!app
            .filter_node_types
            .contains(&crate::graph::types::NodeType::Source));
    }

    #[test]
    fn test_filter_toggle_exposures() {
        let mut app = test_app();
        app.mode = AppMode::Filter;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('e'))));
        assert!(!app
            .filter_node_types
            .contains(&crate::graph::types::NodeType::Exposure));
    }

    #[test]
    fn test_filter_toggle_tests() {
        let mut app = test_app();
        app.mode = AppMode::Filter;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('t'))));
        assert!(!app
            .filter_node_types
            .contains(&crate::graph::types::NodeType::Test));
    }

    #[test]
    fn test_filter_toggle_seeds() {
        let mut app = test_app();
        app.mode = AppMode::Filter;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('d'))));
        assert!(!app
            .filter_node_types
            .contains(&crate::graph::types::NodeType::Seed));
    }

    #[test]
    fn test_filter_status_errored() {
        let mut app = test_app();
        app.mode = AppMode::Filter;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('1'))));
        assert_eq!(app.filter_status, Some(FilterStatus::Errored));
    }

    #[test]
    fn test_filter_status_success() {
        let mut app = test_app();
        app.mode = AppMode::Filter;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('2'))));
        assert_eq!(app.filter_status, Some(FilterStatus::Success));
    }

    #[test]
    fn test_filter_status_never_run() {
        let mut app = test_app();
        app.mode = AppMode::Filter;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('3'))));
        assert_eq!(app.filter_status, Some(FilterStatus::NeverRun));
    }

    #[test]
    fn test_filter_status_clear() {
        let mut app = test_app();
        app.mode = AppMode::Filter;
        app.filter_status = Some(FilterStatus::Errored);
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('0'))));
        assert!(app.filter_status.is_none());
    }

    // ─── Path highlighting tests ───

    #[test]
    fn test_normal_p_toggles_path() {
        let mut app = test_app();
        assert!(app.highlighted_path.is_empty());
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('p'))));
        assert!(!app.highlighted_path.is_empty());
        // Press again to clear
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('p'))));
        assert!(app.highlighted_path.is_empty());
    }

    #[test]
    fn test_path_highlight_no_selection() {
        let mut app = test_app();
        app.selected_node = None;
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('p'))));
        assert!(app.highlighted_path.is_empty());
    }

    // ─── Column lineage tests ───

    #[test]
    fn test_shift_c_toggles_column_lineage() {
        let mut app = test_app();
        assert!(!app.show_column_lineage);
        assert!(!handle_key_event(
            &mut app,
            key_shift(KeyCode::Char('C'))
        ));
        assert!(app.show_column_lineage);
        assert!(!handle_key_event(
            &mut app,
            key_shift(KeyCode::Char('C'))
        ));
        assert!(!app.show_column_lineage);
    }

    // ─── Impact report via path highlight tests ───

    #[test]
    fn test_path_highlight_includes_impact() {
        let mut app = test_app();
        assert!(app.impact_report.is_none());
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('p'))));
        assert!(app.impact_report.is_some());
        // Clear
        assert!(!handle_key_event(&mut app, key(KeyCode::Char('p'))));
        assert!(app.impact_report.is_none());
    }
}
