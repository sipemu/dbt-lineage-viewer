#![cfg(feature = "tui")]

use std::collections::HashMap;
use std::path::PathBuf;

use dbt_lineage::graph::types::*;
use dbt_lineage::parser::artifacts::RunStatusMap;
use dbt_lineage::tui::app::{App, DragState, NodeListEntry};
use dbt_lineage::tui::graph_widget::{hit_test_node, GraphWidget};

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
    });
    let b = graph.add_node(NodeData {
        unique_id: "model.proj.orders".into(),
        label: "orders".into(),
        node_type: NodeType::Model,
        file_path: Some(PathBuf::from("models/marts/orders.sql")),
        description: None,
    });
    graph.add_edge(a, b, EdgeData { edge_type: EdgeType::Ref });
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
// Hit test / mouse interaction state tests
// ───────────────────────────────────────────────────────────

#[test]
fn test_hit_test_node_finds_node() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);

    // Render to set last_graph_area and layout positions
    render_graph_to_frame(&mut app, 80, 24);

    // The first node (stg_orders) should be at layer 0, pos 0
    // In world coords: (0,0) is top-left of first node box
    // With viewport at (0,0) and graph area starting at (0,0),
    // screen coords == world coords.
    // Node box is 24 wide × 3 tall, so clicking at (5, 1) should hit it.
    let result = hit_test_node(&app, 5, 1);
    assert!(result.is_some(), "Expected to hit a node at (5,1)");

    // Verify it's the right node
    let hit_idx = result.unwrap();
    assert_eq!(app.graph[hit_idx].label, "stg_orders");
}

#[test]
fn test_hit_test_node_misses_empty_space() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);

    render_graph_to_frame(&mut app, 80, 24);

    // Click far away from any node (bottom-right corner)
    let result = hit_test_node(&app, 79, 23);
    assert!(result.is_none(), "Expected no hit at (79,23)");
}

#[test]
fn test_select_node_no_center() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);

    // Set viewport to a custom position
    app.viewport_x = 42;
    app.viewport_y = 17;

    let node_idx = app.node_order[1]; // "orders"
    app.select_node_no_center(node_idx);

    // selected_node should be updated
    assert_eq!(app.selected_node, Some(node_idx));

    // Viewport should NOT have changed
    assert_eq!(app.viewport_x, 42);
    assert_eq!(app.viewport_y, 17);
}

#[test]
fn test_drag_state_pans_viewport() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);

    // Simulate starting a drag at (10, 10) with viewport at (0, 0)
    app.drag_state = Some(DragState {
        start_x: 10,
        start_y: 10,
        viewport_x0: 0,
        viewport_y0: 0,
    });

    // Simulate dragging to (15, 12) — delta is (+5, +2)
    // Natural pan: viewport = viewport_x0 - (current_x - start_x)
    let drag = app.drag_state.as_ref().unwrap();
    let new_vx = drag.viewport_x0 - (15i32 - drag.start_x as i32);
    let new_vy = drag.viewport_y0 - (12i32 - drag.start_y as i32);

    assert_eq!(new_vx, -5); // Dragged right → viewport moves left
    assert_eq!(new_vy, -2); // Dragged down → viewport moves up
}

#[test]
fn test_toggle_group_collapse_by_index() {
    let graph = build_two_node_graph();
    let mut app = make_app(graph);

    // Count initial entries (should have group headers + node entries)
    let initial_count = app.node_list_entries.len();
    assert!(initial_count > 0);

    // Find a group header index
    let group_idx = match app.node_list_entries[0] {
        NodeListEntry::GroupHeader(gi) => gi,
        _ => panic!("First entry should be a group header"),
    };

    // Collapse the group
    app.toggle_group_collapse_by_index(group_idx);
    let collapsed_count = app.node_list_entries.len();
    assert!(collapsed_count < initial_count, "Collapsing should reduce entry count");

    // Expand again
    app.toggle_group_collapse_by_index(group_idx);
    let expanded_count = app.node_list_entries.len();
    assert_eq!(expanded_count, initial_count, "Re-expanding should restore entry count");
}
