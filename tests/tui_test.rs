#![cfg(feature = "tui")]

use std::collections::HashMap;
use std::path::PathBuf;

use dbt_lineage::graph::types::*;
use dbt_lineage::parser::artifacts::RunStatusMap;
use dbt_lineage::tui::app::{App, AppMode, DbtRunState, DragState, NodeListEntry};
use dbt_lineage::tui::graph_widget::{hit_test_node, GraphWidget};
use dbt_lineage::tui::ui::draw_ui;

use jugar_probar::tui::{expect_frame, TuiFrame};
use ratatui::backend::TestBackend;
use ratatui::Terminal;

/// Helper: build a minimal 2-node graph (A → B) for testing.
fn build_two_node_graph() -> LineageGraph {
    let mut graph = LineageGraph::new();
    let a = graph.add_node(NodeData {
        unique_id: "model.proj.stg_orders".into(),
        label: "stg_orders".into(),
        node_type: NodeType::Model,
        file_path: Some(PathBuf::from("models/staging/stg_orders.sql")),
        description: None,
        materialization: None,
        tags: vec![],
        columns: vec![],
    });
    let b = graph.add_node(NodeData {
        unique_id: "model.proj.orders".into(),
        label: "orders".into(),
        node_type: NodeType::Model,
        file_path: Some(PathBuf::from("models/marts/orders.sql")),
        description: Some("Final orders model".into()),
        materialization: None,
        tags: vec![],
        columns: vec![],
    });
    graph.add_edge(
        a,
        b,
        EdgeData {
            edge_type: EdgeType::Ref,
        },
    );
    graph
}

/// Helper: build a richer 4-node graph
fn build_four_node_graph() -> LineageGraph {
    let mut graph = LineageGraph::new();
    let src = graph.add_node(NodeData {
        unique_id: "source.raw.orders".into(),
        label: "raw.orders".into(),
        node_type: NodeType::Source,
        file_path: Some(PathBuf::from("models/schema.yml")),
        description: Some("Raw orders source".into()),
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
        description: Some("Final orders mart".into()),
        materialization: None,
        tags: vec![],
        columns: vec![],
    });
    let exp = graph.add_node(NodeData {
        unique_id: "exposure.dashboard".into(),
        label: "dashboard".into(),
        node_type: NodeType::Exposure,
        file_path: None,
        description: Some("Analytics dashboard".into()),
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

/// Helper: create an App from a graph with default run status.
fn make_app(graph: LineageGraph) -> App {
    let run_status: RunStatusMap = HashMap::new();
    App::new(graph, PathBuf::from("/tmp/test_project"), run_status)
}

/// Render GraphWidget into a ratatui TestBackend buffer, then convert to TuiFrame.
fn render_graph_to_frame(app: &mut App, width: u16, height: u16) -> TuiFrame {
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal
        .draw(|f| {
            let area = f.area();
            app.last_graph_area = Some(area);
            f.render_widget(GraphWidget::new(app), area);
        })
        .unwrap();

    // Convert ratatui buffer to TuiFrame lines
    let buf = terminal.backend().buffer();
    let mut lines = Vec::new();
    for y in 0..height {
        let mut line = String::new();
        for x in 0..width {
            let cell = &buf[(x, y)];
            line.push_str(cell.symbol());
        }
        // Trim trailing spaces for cleaner comparison
        let trimmed = line.trim_end().to_string();
        lines.push(trimmed);
    }
    let line_refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
    TuiFrame::from_lines(&line_refs)
}

/// Render draw_ui into a TuiFrame
fn render_full_ui(app: &mut App, width: u16, height: u16) -> TuiFrame {
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(|f| draw_ui(f, app)).unwrap();

    let buf = terminal.backend().buffer();
    let mut lines = Vec::new();
    for y in 0..height {
        let mut line = String::new();
        for x in 0..width {
            let cell = &buf[(x, y)];
            line.push_str(cell.symbol());
        }
        let trimmed = line.trim_end().to_string();
        lines.push(trimmed);
    }
    let line_refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
    TuiFrame::from_lines(&line_refs)
}

// ───────────────────────────────────────────────────────────
// Rendering tests
// ───────────────────────────────────────────────────────────

#[test]
fn test_graph_widget_renders_node_boxes() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    let frame = render_graph_to_frame(&mut app, 80, 24);

    // Node boxes should contain box-drawing characters
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("┌").unwrap();
    assertion.to_contain_text("┐").unwrap();
    assertion.to_contain_text("└").unwrap();
    assertion.to_contain_text("┘").unwrap();

    // Node labels should be visible
    assertion.to_contain_text("stg_orders").unwrap();
    assertion.to_contain_text("orders").unwrap();
}

#[test]
fn test_graph_widget_renders_edges() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    let frame = render_graph_to_frame(&mut app, 80, 24);

    // Edges use horizontal lines and arrowheads
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("─").unwrap();
    assertion.to_contain_text("▸").unwrap();
}

#[test]
fn test_selected_node_highlight() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);

    // Select a specific node
    let node_idx = app.node_order[1]; // "orders"
    app.selected_node = Some(node_idx);

    let frame = render_graph_to_frame(&mut app, 80, 24);

    // The selected node label should appear in the frame
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("orders").unwrap();
}

#[test]
fn test_empty_graph_renders() {
    let graph = LineageGraph::new();
    let mut app = make_app(graph);

    // Should not panic on empty graph
    let frame = render_graph_to_frame(&mut app, 80, 24);
    assert!(frame.height() > 0);
}

// ───────────────────────────────────────────────────────────
// Full UI rendering tests
// ───────────────────────────────────────────────────────────

#[test]
fn test_full_ui_normal_mode() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    let frame = render_full_ui(&mut app, 120, 30);

    let mut assertion = expect_frame(&frame);
    // Should show the graph and detail panel
    assertion.to_contain_text("Lineage Graph").unwrap();
    assertion.to_contain_text("Details").unwrap();
    // Help bar should be visible
    assertion.to_contain_text("hjkl").unwrap();
}

#[test]
fn test_full_ui_with_selection() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    // Select the second node (orders)
    let node_idx = app.node_order[1];
    app.selected_node = Some(node_idx);

    let frame = render_full_ui(&mut app, 120, 30);
    let mut assertion = expect_frame(&frame);
    // Detail panel should show the selected node
    assertion.to_contain_text("Name:").unwrap();
    assertion.to_contain_text("Type:").unwrap();
    assertion.to_contain_text("ID:").unwrap();
}

#[test]
fn test_full_ui_no_selection() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    app.selected_node = None;

    let frame = render_full_ui(&mut app, 120, 30);
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("No node selected").unwrap();
}

#[test]
fn test_full_ui_with_node_list() {
    let graph = build_four_node_graph();
    let mut app = make_app(graph);
    app.show_node_list = true;

    let frame = render_full_ui(&mut app, 120, 30);
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("Nodes").unwrap();
}

#[test]
fn test_full_ui_search_mode() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    app.mode = AppMode::Search;
    app.search_query = "ord".into();

    let frame = render_full_ui(&mut app, 120, 30);
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("Search:").unwrap();
    assertion.to_contain_text("ord").unwrap();
}

#[test]
fn test_full_ui_run_menu() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    app.mode = AppMode::RunMenu;

    let frame = render_full_ui(&mut app, 120, 30);
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("dbt run").unwrap();
    assertion.to_contain_text("dbt test").unwrap();
}

#[test]
fn test_full_ui_run_confirm() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    app.mode = AppMode::RunConfirm;
    app.pending_run = Some(dbt_lineage::tui::runner::DbtRunRequest {
        command: dbt_lineage::tui::runner::DbtCommand::Run,
        scope: dbt_lineage::tui::runner::SelectionScope::Single,
        model_name: "orders".into(),
        project_dir: PathBuf::from("/tmp"),
        use_uv: false,
    });

    let frame = render_full_ui(&mut app, 120, 30);
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("Confirm").unwrap();
    assertion.to_contain_text("Execute").unwrap();
    assertion.to_contain_text("Cancel").unwrap();
}

#[test]
fn test_full_ui_run_output_running() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    let (_tx, rx) = std::sync::mpsc::channel();
    app.run_state = DbtRunState::Running {
        receiver: rx,
        output_lines: vec!["Running dbt...".into()],
    };
    app.mode = AppMode::RunOutput;

    let frame = render_full_ui(&mut app, 120, 30);
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("running").unwrap();
}

#[test]
fn test_full_ui_run_output_success() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    app.run_state = DbtRunState::Finished {
        output_lines: vec!["Completed successfully".into()],
        success: true,
    };
    app.mode = AppMode::RunOutput;

    let frame = render_full_ui(&mut app, 120, 30);
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("success").unwrap();
}

#[test]
fn test_full_ui_run_output_failure() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    app.run_state = DbtRunState::Finished {
        output_lines: vec!["Compilation Error".into()],
        success: false,
    };
    app.mode = AppMode::RunOutput;

    let frame = render_full_ui(&mut app, 120, 30);
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("failed").unwrap();
}

#[test]
fn test_full_ui_context_menu() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    app.mode = AppMode::ContextMenu;
    app.context_menu_pos = Some((30, 10));

    let frame = render_full_ui(&mut app, 120, 30);
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("dbt run").unwrap();
}

#[test]
fn test_full_ui_detail_with_description() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    // Select the node with a description (orders)
    let orders_idx = app
        .graph
        .node_indices()
        .find(|&i| app.graph[i].label == "orders")
        .unwrap();
    app.selected_node = Some(orders_idx);

    let frame = render_full_ui(&mut app, 120, 30);
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("Description:").unwrap();
}

#[test]
fn test_full_ui_detail_shows_upstream_downstream() {
    let graph = build_four_node_graph();
    let mut app = make_app(graph);
    // Select stg_orders which has both upstream and downstream
    let stg_idx = app
        .graph
        .node_indices()
        .find(|&i| app.graph[i].label == "stg_orders")
        .unwrap();
    app.selected_node = Some(stg_idx);

    let frame = render_full_ui(&mut app, 120, 30);
    let mut assertion = expect_frame(&frame);
    assertion.to_contain_text("Upstream:").unwrap();
    assertion.to_contain_text("Downstream:").unwrap();
}

// ───────────────────────────────────────────────────────────
// Hit test / mouse interaction state tests
// ───────────────────────────────────────────────────────────

#[test]
fn test_hit_test_node_finds_node() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);

    render_graph_to_frame(&mut app, 80, 24);

    let result = hit_test_node(&app, 5, 1);
    assert!(result.is_some(), "Expected to hit a node at (5,1)");

    let hit_idx = result.unwrap();
    assert_eq!(app.graph[hit_idx].label, "stg_orders");
}

#[test]
fn test_hit_test_node_misses_empty_space() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);

    render_graph_to_frame(&mut app, 80, 24);

    let result = hit_test_node(&app, 79, 23);
    assert!(result.is_none(), "Expected no hit at (79,23)");
}

#[test]
fn test_select_node_no_center() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);

    app.viewport_x = 42;
    app.viewport_y = 17;

    let node_idx = app.node_order[1];
    app.select_node_no_center(node_idx);

    assert_eq!(app.selected_node, Some(node_idx));
    assert_eq!(app.viewport_x, 42);
    assert_eq!(app.viewport_y, 17);
}

#[test]
fn test_drag_state_pans_viewport() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);

    app.drag_state = Some(DragState {
        start_x: 10,
        start_y: 10,
        viewport_x0: 0,
        viewport_y0: 0,
    });

    let drag = app.drag_state.as_ref().unwrap();
    let new_vx = drag.viewport_x0 - (15i32 - drag.start_x as i32);
    let new_vy = drag.viewport_y0 - (12i32 - drag.start_y as i32);

    assert_eq!(new_vx, -5);
    assert_eq!(new_vy, -2);
}

#[test]
fn test_toggle_group_collapse_by_index() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);

    let initial_count = app.node_list_entries.len();
    assert!(initial_count > 0);

    let group_idx = match app.node_list_entries[0] {
        NodeListEntry::GroupHeader(gi) => gi,
        _ => panic!("First entry should be a group header"),
    };

    app.toggle_group_collapse_by_index(group_idx);
    let collapsed_count = app.node_list_entries.len();
    assert!(
        collapsed_count < initial_count,
        "Collapsing should reduce entry count"
    );

    app.toggle_group_collapse_by_index(group_idx);
    let expanded_count = app.node_list_entries.len();
    assert_eq!(
        expanded_count, initial_count,
        "Re-expanding should restore entry count"
    );
}

// ───────────────────────────────────────────────────────────
// Context menu tests
// ───────────────────────────────────────────────────────────

#[test]
fn test_right_click_opens_context_menu() {
    use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
    use dbt_lineage::tui::event::handle_mouse_event;

    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    render_graph_to_frame(&mut app, 80, 24);

    let mouse = MouseEvent {
        kind: MouseEventKind::Down(MouseButton::Right),
        column: 5,
        row: 1,
        modifiers: crossterm::event::KeyModifiers::NONE,
    };
    handle_mouse_event(&mut app, mouse);

    assert_eq!(app.mode, AppMode::ContextMenu);
    assert!(app.context_menu_pos.is_some());
    assert!(app.selected_node.is_some());
    assert_eq!(app.graph[app.selected_node.unwrap()].label, "stg_orders");
}

#[test]
fn test_right_click_empty_space_does_nothing() {
    use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
    use dbt_lineage::tui::event::handle_mouse_event;

    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    render_graph_to_frame(&mut app, 80, 24);

    let mouse = MouseEvent {
        kind: MouseEventKind::Down(MouseButton::Right),
        column: 79,
        row: 23,
        modifiers: crossterm::event::KeyModifiers::NONE,
    };
    handle_mouse_event(&mut app, mouse);

    assert_eq!(app.mode, AppMode::Normal);
    assert!(app.context_menu_pos.is_none());
}

#[test]
fn test_context_menu_dismissed_by_click() {
    use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
    use dbt_lineage::tui::event::handle_mouse_event;

    let graph = build_two_node_graph();
    let mut app = make_app(graph);
    render_graph_to_frame(&mut app, 80, 24);

    let mouse = MouseEvent {
        kind: MouseEventKind::Down(MouseButton::Right),
        column: 5,
        row: 1,
        modifiers: crossterm::event::KeyModifiers::NONE,
    };
    handle_mouse_event(&mut app, mouse);
    assert_eq!(app.mode, AppMode::ContextMenu);

    let dismiss = MouseEvent {
        kind: MouseEventKind::Down(MouseButton::Left),
        column: 40,
        row: 12,
        modifiers: crossterm::event::KeyModifiers::NONE,
    };
    handle_mouse_event(&mut app, dismiss);
    assert_eq!(app.mode, AppMode::Normal);
    assert!(app.context_menu_pos.is_none());
}
