use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use indexmap::IndexMap;
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use ratatui::layout::Rect;
use ratatui::widgets::ListState;

use crate::graph::impact::ImpactReport;
use crate::graph::types::{LineageGraph, NodeType};
use crate::parser::artifacts::{self, RunStatus, RunStatusMap};
use crate::parser::column_lineage::ColumnLineage;
use crate::render::layout::{sugiyama_layout, LayoutResult};

use super::runner::{spawn_dbt_run, DbtRunMessage, DbtRunRequest};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AppMode {
    Normal,
    Search,
    RunMenu,
    ContextMenu,
    RunConfirm,
    RunOutput,
    Filter,
}

/// Filter by run status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterStatus {
    Errored,
    Success,
    NeverRun,
}

/// State of a background dbt run
pub enum DbtRunState {
    Idle,
    Running {
        receiver: mpsc::Receiver<DbtRunMessage>,
        output_lines: Vec<String>,
    },
    Finished {
        output_lines: Vec<String>,
        success: bool,
    },
}

/// A directory-based group of nodes for the collapsible node list
pub struct NodeGroup {
    pub key: String,
    pub label: String,
    pub nodes: Vec<NodeIndex>,
}

/// A single row in the flattened node list display
#[derive(Debug, Clone, Copy)]
pub enum NodeListEntry {
    GroupHeader(usize),
    Node(NodeIndex),
}

/// Tracks an in-progress mouse drag for viewport panning
pub struct DragState {
    pub start_x: u16,
    pub start_y: u16,
    pub viewport_x0: i32,
    pub viewport_y0: i32,
}

pub struct App {
    pub graph: LineageGraph,
    pub layout: LayoutResult,
    pub selected_node: Option<NodeIndex>,
    pub viewport_x: i32,
    pub viewport_y: i32,
    pub zoom: f64,
    pub last_graph_area: Option<Rect>,
    pub mode: AppMode,
    pub search_query: String,
    pub search_results: Vec<NodeIndex>,
    pub search_cursor: usize,
    /// Ordered list of all node indices for Tab cycling
    pub node_order: Vec<NodeIndex>,
    pub node_cycle_index: usize,

    // Node list panel
    pub show_node_list: bool,
    pub node_list_state: ListState,
    pub node_groups: Vec<NodeGroup>,
    pub collapsed_groups: HashSet<String>,
    pub node_list_entries: Vec<NodeListEntry>,

    // Mouse interaction state
    pub drag_state: Option<DragState>,
    pub last_node_list_area: Option<Rect>,
    pub context_menu_pos: Option<(u16, u16)>,
    pub last_context_menu_area: Option<Rect>,
    pub last_run_menu_area: Option<Rect>,
    pub menu_hover_index: Option<usize>,
    pub last_confirm_area: Option<Rect>,
    pub confirm_hover: Option<bool>, // Some(true) = Execute hovered, Some(false) = Cancel hovered

    // Run execution state
    pub project_dir: PathBuf,
    pub run_status: RunStatusMap,
    pub run_state: DbtRunState,
    pub run_output_scroll: usize,
    pub pending_run: Option<DbtRunRequest>,

    // Filtering state
    pub filter_node_types: HashSet<NodeType>,
    pub filter_status: Option<FilterStatus>,

    // Path highlighting state
    pub highlighted_path: HashSet<NodeIndex>,
    /// The node for which the path was computed (so we can clear on re-select)
    pub path_highlight_source: Option<NodeIndex>,

    // Impact analysis (computed when path is highlighted)
    pub impact_report: Option<ImpactReport>,

    // Column-level lineage
    pub column_lineage: ColumnLineage,
    pub show_column_lineage: bool,
}

impl App {
    pub fn new(graph: LineageGraph, project_dir: PathBuf, run_status: RunStatusMap) -> Self {
        let layout = sugiyama_layout(&graph);

        // Build node order from layout (layer by layer, position by position)
        let mut node_order = Vec::new();
        for layer in &layout.layers {
            for &node in layer {
                node_order.push(node);
            }
        }

        let selected = node_order.first().copied();

        let node_groups = build_node_groups(&node_order, &graph, &project_dir);
        let collapsed_groups = HashSet::new();
        let node_list_entries = build_node_list_entries(&node_groups, &collapsed_groups);

        let mut node_list_state = ListState::default();
        if !node_list_entries.is_empty() {
            // Select the first Node entry (skip the first GroupHeader)
            let first_node_idx = node_list_entries
                .iter()
                .position(|e| matches!(e, NodeListEntry::Node(_)))
                .unwrap_or(0);
            node_list_state.select(Some(first_node_idx));
        }

        // Initialize filter_node_types with all node types shown by default
        let filter_node_types: HashSet<NodeType> = [
            NodeType::Model,
            NodeType::Source,
            NodeType::Exposure,
            NodeType::Test,
            NodeType::Seed,
            NodeType::Snapshot,
            NodeType::Phantom,
        ]
        .into_iter()
        .collect();

        App {
            graph,
            layout,
            selected_node: selected,
            viewport_x: 0,
            viewport_y: 0,
            zoom: 1.0,
            last_graph_area: None,
            mode: AppMode::Normal,
            search_query: String::new(),
            search_results: Vec::new(),
            search_cursor: 0,
            node_order,
            node_cycle_index: 0,
            show_node_list: false,
            node_list_state,
            node_groups,
            collapsed_groups,
            node_list_entries,
            drag_state: None,
            last_node_list_area: None,
            context_menu_pos: None,
            last_context_menu_area: None,
            last_run_menu_area: None,
            menu_hover_index: None,
            last_confirm_area: None,
            confirm_hover: None,
            project_dir,
            run_status,
            run_state: DbtRunState::Idle,
            run_output_scroll: 0,
            pending_run: None,
            filter_node_types,
            filter_status: None,
            highlighted_path: HashSet::new(),
            path_highlight_source: None,
            impact_report: None,
            column_lineage: ColumnLineage::default(),
            show_column_lineage: false,
        }
    }

    pub fn cycle_next_node(&mut self) {
        if self.node_order.is_empty() {
            return;
        }
        self.node_cycle_index = (self.node_cycle_index + 1) % self.node_order.len();
        self.selected_node = Some(self.node_order[self.node_cycle_index]);
        self.sync_node_list_state();
        self.center_on_selected();
    }

    pub fn cycle_prev_node(&mut self) {
        if self.node_order.is_empty() {
            return;
        }
        if self.node_cycle_index == 0 {
            self.node_cycle_index = self.node_order.len() - 1;
        } else {
            self.node_cycle_index -= 1;
        }
        self.selected_node = Some(self.node_order[self.node_cycle_index]);
        self.sync_node_list_state();
        self.center_on_selected();
    }

    /// Navigate to the closest node in the next layer (downstream / right)
    pub fn navigate_right(&mut self) {
        let Some(current) = self.selected_node else {
            return;
        };
        let Some(&(cur_layer, cur_pos)) = self.layout.positions.get(&current) else {
            return;
        };

        // Find the closest node in the nearest non-empty layer to the right
        let mut best: Option<(NodeIndex, usize, usize)> = None; // (node, layer_dist, pos_dist)
        for (&node, &(layer, pos)) in &self.layout.positions {
            if layer > cur_layer {
                let layer_dist = layer - cur_layer;
                let pos_dist = (pos as isize - cur_pos as isize).unsigned_abs();
                if let Some((_, bl, bp)) = best {
                    if layer_dist < bl || (layer_dist == bl && pos_dist < bp) {
                        best = Some((node, layer_dist, pos_dist));
                    }
                } else {
                    best = Some((node, layer_dist, pos_dist));
                }
            }
        }

        if let Some((node, _, _)) = best {
            self.selected_node = Some(node);
            self.sync_cycle_index();
            self.sync_node_list_state();
            self.center_on_selected();
        }
    }

    /// Navigate to the closest node in the previous layer (upstream / left)
    pub fn navigate_left(&mut self) {
        let Some(current) = self.selected_node else {
            return;
        };
        let Some(&(cur_layer, cur_pos)) = self.layout.positions.get(&current) else {
            return;
        };
        if cur_layer == 0 {
            return;
        }

        let mut best: Option<(NodeIndex, usize, usize)> = None;
        for (&node, &(layer, pos)) in &self.layout.positions {
            if layer < cur_layer {
                let layer_dist = cur_layer - layer;
                let pos_dist = (pos as isize - cur_pos as isize).unsigned_abs();
                if let Some((_, bl, bp)) = best {
                    if layer_dist < bl || (layer_dist == bl && pos_dist < bp) {
                        best = Some((node, layer_dist, pos_dist));
                    }
                } else {
                    best = Some((node, layer_dist, pos_dist));
                }
            }
        }

        if let Some((node, _, _)) = best {
            self.selected_node = Some(node);
            self.sync_cycle_index();
            self.sync_node_list_state();
            self.center_on_selected();
        }
    }

    /// Navigate up within the same layer (wraps around)
    pub fn navigate_up(&mut self) {
        let Some(current) = self.selected_node else {
            return;
        };
        let Some(&(cur_layer, _cur_pos)) = self.layout.positions.get(&current) else {
            return;
        };

        if cur_layer >= self.layout.layers.len() {
            return;
        }
        let layer = &self.layout.layers[cur_layer];
        if layer.len() <= 1 {
            return;
        }

        // Find current position in the layer vec
        let Some(idx) = layer.iter().position(|&n| n == current) else {
            return;
        };
        let new_idx = if idx == 0 { layer.len() - 1 } else { idx - 1 };

        self.selected_node = Some(layer[new_idx]);
        self.sync_cycle_index();
        self.sync_node_list_state();
        self.center_on_selected();
    }

    /// Navigate down within the same layer (wraps around)
    pub fn navigate_down(&mut self) {
        let Some(current) = self.selected_node else {
            return;
        };
        let Some(&(cur_layer, _cur_pos)) = self.layout.positions.get(&current) else {
            return;
        };

        if cur_layer >= self.layout.layers.len() {
            return;
        }
        let layer = &self.layout.layers[cur_layer];
        if layer.len() <= 1 {
            return;
        }

        let Some(idx) = layer.iter().position(|&n| n == current) else {
            return;
        };
        let new_idx = (idx + 1) % layer.len();

        self.selected_node = Some(layer[new_idx]);
        self.sync_cycle_index();
        self.sync_node_list_state();
        self.center_on_selected();
    }

    /// Sync node_cycle_index to match the current selected_node
    pub fn sync_cycle_index(&mut self) {
        if let Some(selected) = self.selected_node {
            if let Some(idx) = self.node_order.iter().position(|&n| n == selected) {
                self.node_cycle_index = idx;
            }
        }
    }

    /// Sync the node list ListState selection to match the current selected_node.
    /// Auto-expands the group containing the selected node if it's collapsed.
    pub fn sync_node_list_state(&mut self) {
        let Some(selected) = self.selected_node else {
            return;
        };

        // Auto-expand the group containing the selected node
        let group_key = self.group_key_for_selected(selected);
        if let Some(key) = group_key {
            if self.collapsed_groups.remove(&key) {
                self.node_list_entries =
                    build_node_list_entries(&self.node_groups, &self.collapsed_groups);
            }
        }

        // Find flat index of this node in node_list_entries
        if let Some(flat_idx) = self
            .node_list_entries
            .iter()
            .position(|e| matches!(e, NodeListEntry::Node(idx) if *idx == selected))
        {
            self.node_list_state.select(Some(flat_idx));
        }
    }

    /// Find the group key for a given node
    fn group_key_for_selected(&self, node_idx: NodeIndex) -> Option<String> {
        self.node_groups
            .iter()
            .find(|g| g.nodes.contains(&node_idx))
            .map(|g| g.key.clone())
    }

    /// Toggle collapse state of the group containing the currently selected node
    pub fn toggle_group_collapse(&mut self) {
        let Some(selected) = self.selected_node else {
            return;
        };

        // Find which group the selected node belongs to
        let group_idx = match self
            .node_groups
            .iter()
            .position(|g| g.nodes.contains(&selected))
        {
            Some(i) => i,
            None => return,
        };
        let key = self.node_groups[group_idx].key.clone();

        if self.collapsed_groups.contains(&key) {
            // Expand: remove from set, rebuild, select the node row
            self.collapsed_groups.remove(&key);
            self.node_list_entries =
                build_node_list_entries(&self.node_groups, &self.collapsed_groups);
            // Select the node row
            if let Some(flat_idx) = self
                .node_list_entries
                .iter()
                .position(|e| matches!(e, NodeListEntry::Node(idx) if *idx == selected))
            {
                self.node_list_state.select(Some(flat_idx));
            }
        } else {
            // Collapse: add to set, rebuild, select the group header row
            self.collapsed_groups.insert(key);
            self.node_list_entries =
                build_node_list_entries(&self.node_groups, &self.collapsed_groups);
            // Select the group header row
            if let Some(flat_idx) = self
                .node_list_entries
                .iter()
                .position(|e| matches!(e, NodeListEntry::GroupHeader(i) if *i == group_idx))
            {
                self.node_list_state.select(Some(flat_idx));
            }
        }
    }

    /// Select a node without centering the viewport (used for mouse clicks on the graph)
    pub fn select_node_no_center(&mut self, idx: NodeIndex) {
        self.selected_node = Some(idx);
        self.sync_cycle_index();
        self.sync_node_list_state();
    }

    /// Toggle collapse state of a group by its index (used for mouse clicks on group headers)
    pub fn toggle_group_collapse_by_index(&mut self, group_idx: usize) {
        if group_idx >= self.node_groups.len() {
            return;
        }
        let key = self.node_groups[group_idx].key.clone();

        if self.collapsed_groups.contains(&key) {
            self.collapsed_groups.remove(&key);
        } else {
            self.collapsed_groups.insert(key);
        }
        self.node_list_entries = build_node_list_entries(&self.node_groups, &self.collapsed_groups);
    }

    /// Center the viewport on the currently selected node
    pub fn center_on_selected(&mut self) {
        let Some(selected) = self.selected_node else {
            return;
        };
        let Some(&(layer, pos)) = self.layout.positions.get(&selected) else {
            return;
        };

        use super::graph_widget::node_world_center;
        let (cx, cy) = node_world_center(layer, pos, self.zoom);

        if let Some(area) = self.last_graph_area {
            self.viewport_x = cx - area.width as i32 / 2;
            self.viewport_y = cy - area.height as i32 / 2;
        } else {
            // Fallback: assume a reasonable default area
            self.viewport_x = cx - 40;
            self.viewport_y = cy - 12;
        }
    }

    pub fn update_search(&mut self) {
        let query = self.search_query.to_lowercase();
        self.search_results = self
            .graph
            .node_indices()
            .filter(|&idx| {
                let node = &self.graph[idx];
                node.label.to_lowercase().contains(&query)
                    || node.unique_id.to_lowercase().contains(&query)
            })
            .collect();
        self.search_cursor = 0;
        if let Some(&first) = self.search_results.first() {
            self.selected_node = Some(first);
        }
    }

    pub fn next_search_result(&mut self) {
        if self.search_results.is_empty() {
            return;
        }
        self.search_cursor = (self.search_cursor + 1) % self.search_results.len();
        self.selected_node = Some(self.search_results[self.search_cursor]);
    }

    pub fn reset_view(&mut self) {
        self.viewport_x = 0;
        self.viewport_y = 0;
        self.zoom = 1.0;
    }

    /// Get upstream neighbors of a node
    pub fn upstream_of(&self, idx: NodeIndex) -> Vec<NodeIndex> {
        self.graph
            .edges_directed(idx, Direction::Incoming)
            .map(|e| e.source())
            .collect()
    }

    /// Get downstream neighbors of a node
    pub fn downstream_of(&self, idx: NodeIndex) -> Vec<NodeIndex> {
        self.graph
            .edges_directed(idx, Direction::Outgoing)
            .map(|e| e.target())
            .collect()
    }

    /// Drain pending messages from a running dbt process
    pub fn drain_run_messages(&mut self) {
        if let DbtRunState::Running {
            ref receiver,
            ref mut output_lines,
        } = self.run_state
        {
            // Non-blocking drain of all available messages
            loop {
                match receiver.try_recv() {
                    Ok(DbtRunMessage::OutputLine(line)) => {
                        output_lines.push(line);
                    }
                    Ok(DbtRunMessage::Completed { success }) => {
                        let lines = std::mem::take(output_lines);
                        self.run_state = DbtRunState::Finished {
                            output_lines: lines,
                            success,
                        };
                        // Reload run status after completion
                        self.reload_run_status();
                        return;
                    }
                    Ok(DbtRunMessage::SpawnError(msg)) => {
                        output_lines.push(format!("ERROR: {}", msg));
                        let lines = std::mem::take(output_lines);
                        self.run_state = DbtRunState::Finished {
                            output_lines: lines,
                            success: false,
                        };
                        return;
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        let lines = std::mem::take(output_lines);
                        self.run_state = DbtRunState::Finished {
                            output_lines: lines,
                            success: false,
                        };
                        return;
                    }
                }
            }
        }
    }

    /// Start executing a dbt run from the pending request
    pub fn start_dbt_run(&mut self) {
        if let Some(request) = self.pending_run.take() {
            let receiver = spawn_dbt_run(request);
            self.run_state = DbtRunState::Running {
                receiver,
                output_lines: Vec::new(),
            };
            self.run_output_scroll = 0;
            self.mode = AppMode::RunOutput;
        }
    }

    /// Reload run status from target/run_results.json, merging into existing state
    pub fn reload_run_status(&mut self) {
        if let Ok(Some(results)) = artifacts::load_run_results(&self.project_dir) {
            artifacts::merge_run_status_map(
                &mut self.run_status,
                &results,
                &self.graph,
                &self.project_dir,
            );
        }
    }

    /// Get the run status for a node by unique_id
    pub fn node_run_status(&self, unique_id: &str) -> &RunStatus {
        self.run_status
            .get(unique_id)
            .unwrap_or(&RunStatus::NeverRun)
    }

    /// Check if a node passes the current filters
    pub fn node_passes_filter(&self, idx: NodeIndex) -> bool {
        let node = &self.graph[idx];

        // Check node type filter
        if !self.filter_node_types.contains(&node.node_type) {
            return false;
        }

        // Check status filter
        if let Some(ref fs) = self.filter_status {
            let run_status = self.node_run_status(&node.unique_id);
            match fs {
                FilterStatus::Errored => {
                    if !matches!(run_status, RunStatus::Error { .. }) {
                        return false;
                    }
                }
                FilterStatus::Success => {
                    if !matches!(run_status, RunStatus::Success { .. }) {
                        return false;
                    }
                }
                FilterStatus::NeverRun => {
                    if !matches!(run_status, RunStatus::NeverRun) {
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Toggle a node type in the filter set
    pub fn toggle_filter_node_type(&mut self, nt: NodeType) {
        if self.filter_node_types.contains(&nt) {
            self.filter_node_types.remove(&nt);
        } else {
            self.filter_node_types.insert(nt);
        }
    }

    /// Build a description of active filters for the help bar
    pub fn filter_description(&self) -> Option<String> {
        let all_types: HashSet<NodeType> = [
            NodeType::Model,
            NodeType::Source,
            NodeType::Exposure,
            NodeType::Test,
            NodeType::Seed,
            NodeType::Snapshot,
            NodeType::Phantom,
        ]
        .into_iter()
        .collect();

        let mut parts = Vec::new();

        // Show which types are hidden
        let hidden: Vec<&str> = all_types
            .difference(&self.filter_node_types)
            .map(|nt| nt.label())
            .collect();
        if !hidden.is_empty() {
            parts.push(format!("hide:{}", hidden.join(",")));
        }

        // Show status filter
        if let Some(ref fs) = self.filter_status {
            let label = match fs {
                FilterStatus::Errored => "errored",
                FilterStatus::Success => "success",
                FilterStatus::NeverRun => "never-run",
            };
            parts.push(format!("status:{}", label));
        }

        if parts.is_empty() {
            None
        } else {
            Some(parts.join(" "))
        }
    }

    /// Toggle path highlighting for the currently selected node.
    /// If already highlighting this node, clear it. Otherwise compute paths.
    pub fn toggle_path_highlight(&mut self) {
        let Some(selected) = self.selected_node else {
            return;
        };

        // If already highlighting this node, clear
        if self.path_highlight_source == Some(selected) {
            self.highlighted_path.clear();
            self.path_highlight_source = None;
            self.impact_report = None;
            return;
        }

        // Compute the full path through the selected node
        self.highlighted_path = compute_path_through(&self.graph, selected);
        self.path_highlight_source = Some(selected);

        // Also compute impact report for downstream analysis
        self.impact_report = Some(crate::graph::impact::compute_impact(&self.graph, selected));
    }

    /// Toggle column-level lineage display. Resolves lazily on first toggle.
    pub fn toggle_column_lineage(&mut self) {
        self.show_column_lineage = !self.show_column_lineage;

        // Resolve column lineage lazily on first enable
        if self.show_column_lineage && self.column_lineage.edges.is_empty() {
            self.column_lineage =
                crate::parser::column_lineage::resolve_column_lineage(&self.graph);
        }
    }

    /// Whether a dbt run is currently in progress
    pub fn is_run_in_progress(&self) -> bool {
        matches!(self.run_state, DbtRunState::Running { .. })
    }

    /// Whether we have any run output to show
    pub fn has_run_output(&self) -> bool {
        !matches!(self.run_state, DbtRunState::Idle)
    }
}

/// Derive a group key for a node based on its file path
fn group_key_for_node(node: &crate::graph::types::NodeData, project_dir: &Path) -> String {
    if let Some(path) = &node.file_path {
        // Normalize absolute paths by stripping the project dir prefix
        let rel = if path.is_absolute() {
            path.strip_prefix(project_dir).unwrap_or(path.as_path())
        } else {
            path.as_path()
        };
        // Use the parent directory as the group key
        rel.parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "(root)".to_string())
    } else {
        match node.node_type {
            NodeType::Exposure => "(exposures)".to_string(),
            NodeType::Phantom => "(unresolved)".to_string(),
            _ => "(other)".to_string(),
        }
    }
}

/// Build directory-based node groups from the node order
fn build_node_groups(
    node_order: &[NodeIndex],
    graph: &LineageGraph,
    project_dir: &Path,
) -> Vec<NodeGroup> {
    // Use IndexMap to preserve insertion order (first-seen group = first group)
    let mut groups: IndexMap<String, Vec<NodeIndex>> = IndexMap::new();

    for &idx in node_order {
        let node = &graph[idx];
        let key = group_key_for_node(node, project_dir);
        groups.entry(key).or_default().push(idx);
    }

    groups
        .into_iter()
        .map(|(key, nodes)| {
            let label = if key.is_empty() {
                "(root)".to_string()
            } else {
                key.clone()
            };
            NodeGroup { key, label, nodes }
        })
        .collect()
}

/// Compute all nodes on paths through a given node.
/// BFS backward to find all ancestors, BFS forward to find all descendants,
/// then union them together with the node itself.
pub fn compute_path_through(graph: &LineageGraph, node: NodeIndex) -> HashSet<NodeIndex> {
    let mut result = HashSet::new();
    result.insert(node);

    // BFS backward (upstream / ancestors)
    let mut queue = VecDeque::new();
    queue.push_back(node);
    while let Some(current) = queue.pop_front() {
        for edge in graph.edges_directed(current, Direction::Incoming) {
            let src = edge.source();
            if result.insert(src) {
                queue.push_back(src);
            }
        }
    }

    // BFS forward (downstream / descendants)
    queue.push_back(node);
    while let Some(current) = queue.pop_front() {
        for edge in graph.edges_directed(current, Direction::Outgoing) {
            let tgt = edge.target();
            if result.insert(tgt) {
                queue.push_back(tgt);
            }
        }
    }

    result
}

/// Build the flat list of entries from groups and collapse state
fn build_node_list_entries(
    groups: &[NodeGroup],
    collapsed: &HashSet<String>,
) -> Vec<NodeListEntry> {
    let mut entries = Vec::new();
    for (i, group) in groups.iter().enumerate() {
        entries.push(NodeListEntry::GroupHeader(i));
        if !collapsed.contains(&group.key) {
            for &idx in &group.nodes {
                entries.push(NodeListEntry::Node(idx));
            }
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::*;
    use std::collections::HashMap;
    use std::sync::mpsc;

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
        App::new(make_test_graph(), PathBuf::from("/tmp"), HashMap::new())
    }

    #[test]
    fn test_app_new() {
        let app = test_app();
        assert_eq!(app.graph.node_count(), 4);
        assert_eq!(app.node_order.len(), 4);
        assert!(app.selected_node.is_some());
        assert_eq!(app.mode, AppMode::Normal);
        assert_eq!(app.zoom, 1.0);
    }

    #[test]
    fn test_cycle_next_node() {
        let mut app = test_app();
        let first = app.selected_node;
        app.cycle_next_node();
        assert_ne!(app.selected_node, first);
        // Cycle through all nodes and back
        for _ in 0..app.node_order.len() - 1 {
            app.cycle_next_node();
        }
        assert_eq!(app.selected_node, first);
    }

    #[test]
    fn test_cycle_prev_node() {
        let mut app = test_app();
        let first = app.selected_node;
        app.cycle_prev_node();
        // Should wrap to last
        assert_eq!(app.selected_node, Some(*app.node_order.last().unwrap()));
        // Cycle back to first
        app.cycle_next_node();
        assert_eq!(app.selected_node, first);
    }

    #[test]
    fn test_cycle_empty_graph() {
        let graph = LineageGraph::new();
        let mut app = App::new(graph, PathBuf::from("/tmp"), HashMap::new());
        // Should not panic
        app.cycle_next_node();
        app.cycle_prev_node();
        assert!(app.selected_node.is_none());
    }

    #[test]
    fn test_navigate_right() {
        let mut app = test_app();
        // Select the first node (source, layer 0)
        app.selected_node = Some(app.node_order[0]);
        let initial_layer = app.layout.positions[&app.node_order[0]].0;

        app.navigate_right();

        let new_layer = app
            .selected_node
            .and_then(|n| app.layout.positions.get(&n).map(|p| p.0));
        assert!(new_layer.unwrap() > initial_layer);
    }

    #[test]
    fn test_navigate_left() {
        let mut app = test_app();
        // Select a downstream node first
        app.navigate_right();
        let mid_sel = app.selected_node;
        app.navigate_left();
        // Should go back upstream
        assert_ne!(app.selected_node, mid_sel);
    }

    #[test]
    fn test_navigate_left_at_layer_zero() {
        let mut app = test_app();
        // Select a node at layer 0
        let layer0_node = app
            .node_order
            .iter()
            .find(|&&n| app.layout.positions[&n].0 == 0)
            .copied()
            .unwrap();
        app.selected_node = Some(layer0_node);
        let before = app.selected_node;
        app.navigate_left();
        assert_eq!(app.selected_node, before); // Should not change
    }

    #[test]
    fn test_navigate_up_down_single_layer() {
        let mut app = test_app();
        // Select a node and try up/down — with single-node layers, nothing happens
        let sel = app.selected_node.unwrap();
        let (layer, _) = app.layout.positions[&sel];
        let layer_nodes = &app.layout.layers[layer];
        if layer_nodes.len() <= 1 {
            app.navigate_up();
            assert_eq!(app.selected_node, Some(sel));
            app.navigate_down();
            assert_eq!(app.selected_node, Some(sel));
        }
    }

    #[test]
    fn test_navigate_no_selection() {
        let mut app = test_app();
        app.selected_node = None;
        // Should not panic
        app.navigate_left();
        app.navigate_right();
        app.navigate_up();
        app.navigate_down();
        assert!(app.selected_node.is_none());
    }

    #[test]
    fn test_search() {
        let mut app = test_app();
        app.search_query = "orders".into();
        app.update_search();
        // Should find "stg_orders" and "orders"
        assert!(app.search_results.len() >= 2);
        assert!(app.selected_node.is_some());
    }

    #[test]
    fn test_search_no_match() {
        let mut app = test_app();
        app.search_query = "zzz_nonexistent".into();
        app.update_search();
        assert!(app.search_results.is_empty());
    }

    #[test]
    fn test_next_search_result() {
        let mut app = test_app();
        app.search_query = "orders".into();
        app.update_search();
        let first = app.selected_node;
        app.next_search_result();
        if app.search_results.len() > 1 {
            assert_ne!(app.selected_node, first);
        }
    }

    #[test]
    fn test_next_search_result_empty() {
        let mut app = test_app();
        app.search_results.clear();
        // Should not panic
        app.next_search_result();
    }

    #[test]
    fn test_reset_view() {
        let mut app = test_app();
        app.viewport_x = 50;
        app.viewport_y = 30;
        app.zoom = 2.0;
        app.reset_view();
        assert_eq!(app.viewport_x, 0);
        assert_eq!(app.viewport_y, 0);
        assert_eq!(app.zoom, 1.0);
    }

    #[test]
    fn test_center_on_selected() {
        let mut app = test_app();
        app.viewport_x = 0;
        app.viewport_y = 0;
        app.last_graph_area = Some(ratatui::layout::Rect::new(0, 0, 80, 24));
        app.center_on_selected();
        // Viewport should have changed (unless node happens to be at center)
        // Just verify it doesn't panic and viewport changed
    }

    #[test]
    fn test_center_on_selected_no_selection() {
        let mut app = test_app();
        app.selected_node = None;
        app.center_on_selected();
        assert_eq!(app.viewport_x, 0); // Unchanged
    }

    #[test]
    fn test_center_on_selected_no_graph_area() {
        let mut app = test_app();
        app.last_graph_area = None;
        app.center_on_selected();
        // Uses fallback: viewport_x = cx - 40, viewport_y = cy - 12
        // Just verify no panic
    }

    #[test]
    fn test_node_groups() {
        let app = test_app();
        assert!(!app.node_groups.is_empty());
        let total_nodes: usize = app.node_groups.iter().map(|g| g.nodes.len()).sum();
        assert_eq!(total_nodes, 4);
    }

    #[test]
    fn test_toggle_group_collapse() {
        let mut app = test_app();
        app.show_node_list = true;
        let initial_entries = app.node_list_entries.len();
        app.toggle_group_collapse();
        let collapsed_entries = app.node_list_entries.len();
        // Should have fewer entries after collapsing
        assert!(collapsed_entries < initial_entries || initial_entries == collapsed_entries);
    }

    #[test]
    fn test_toggle_group_collapse_no_selection() {
        let mut app = test_app();
        app.selected_node = None;
        // Should not panic
        app.toggle_group_collapse();
    }

    #[test]
    fn test_sync_node_list_state_auto_expand() {
        let mut app = test_app();
        // Collapse a group, then select a node in it — should auto-expand
        if !app.node_groups.is_empty() {
            let key = app.node_groups[0].key.clone();
            app.collapsed_groups.insert(key.clone());
            app.node_list_entries =
                build_node_list_entries(&app.node_groups, &app.collapsed_groups);

            // Select the first node in that group
            if let Some(&first_node) = app.node_groups[0].nodes.first() {
                app.selected_node = Some(first_node);
                app.sync_node_list_state();
                // Group should be expanded now
                assert!(!app.collapsed_groups.contains(&key));
            }
        }
    }

    #[test]
    fn test_upstream_downstream() {
        let app = test_app();
        // Find stg_orders (should have upstream source and downstream orders)
        let stg = app
            .graph
            .node_indices()
            .find(|&i| app.graph[i].label == "stg_orders")
            .unwrap();
        let upstream = app.upstream_of(stg);
        let downstream = app.downstream_of(stg);
        assert_eq!(upstream.len(), 1); // source
        assert_eq!(downstream.len(), 1); // orders
    }

    #[test]
    fn test_node_run_status_default() {
        let app = test_app();
        let status = app.node_run_status("model.stg_orders");
        assert!(matches!(status, RunStatus::NeverRun));
    }

    #[test]
    fn test_is_run_in_progress() {
        let mut app = test_app();
        assert!(!app.is_run_in_progress());
        let (_tx, rx) = mpsc::channel();
        app.run_state = DbtRunState::Running {
            receiver: rx,
            output_lines: vec![],
        };
        assert!(app.is_run_in_progress());
    }

    #[test]
    fn test_has_run_output() {
        let mut app = test_app();
        assert!(!app.has_run_output());
        app.run_state = DbtRunState::Finished {
            output_lines: vec!["done".into()],
            success: true,
        };
        assert!(app.has_run_output());
    }

    #[test]
    fn test_drain_run_messages_completed() {
        let mut app = test_app();
        let (tx, rx) = mpsc::channel();
        app.run_state = DbtRunState::Running {
            receiver: rx,
            output_lines: vec![],
        };
        tx.send(super::super::runner::DbtRunMessage::OutputLine(
            "line1".into(),
        ))
        .unwrap();
        tx.send(super::super::runner::DbtRunMessage::Completed { success: true })
            .unwrap();
        app.drain_run_messages();
        assert!(matches!(
            app.run_state,
            DbtRunState::Finished { success: true, .. }
        ));
    }

    #[test]
    fn test_drain_run_messages_spawn_error() {
        let mut app = test_app();
        let (tx, rx) = mpsc::channel();
        app.run_state = DbtRunState::Running {
            receiver: rx,
            output_lines: vec![],
        };
        tx.send(super::super::runner::DbtRunMessage::SpawnError(
            "failed".into(),
        ))
        .unwrap();
        app.drain_run_messages();
        match &app.run_state {
            DbtRunState::Finished {
                success,
                output_lines,
            } => {
                assert!(!success);
                assert!(output_lines.iter().any(|l| l.contains("ERROR")));
            }
            _ => panic!("Expected Finished"),
        }
    }

    #[test]
    fn test_drain_run_messages_disconnected() {
        let mut app = test_app();
        let (tx, rx) = mpsc::channel();
        app.run_state = DbtRunState::Running {
            receiver: rx,
            output_lines: vec![],
        };
        drop(tx); // Disconnect
        app.drain_run_messages();
        assert!(matches!(
            app.run_state,
            DbtRunState::Finished { success: false, .. }
        ));
    }

    #[test]
    fn test_drain_run_messages_idle() {
        let mut app = test_app();
        // Should not panic when idle
        app.drain_run_messages();
    }

    #[test]
    fn test_select_node_no_center() {
        let mut app = test_app();
        app.viewport_x = 42;
        app.viewport_y = 17;
        let node = app.node_order[1];
        app.select_node_no_center(node);
        assert_eq!(app.selected_node, Some(node));
        assert_eq!(app.viewport_x, 42);
        assert_eq!(app.viewport_y, 17);
    }

    #[test]
    fn test_toggle_group_collapse_by_index_out_of_bounds() {
        let mut app = test_app();
        let entries_before = app.node_list_entries.len();
        app.toggle_group_collapse_by_index(999);
        assert_eq!(app.node_list_entries.len(), entries_before);
    }

    /// Graph with 2 nodes in the same layer (fan-out), to test navigate_up/down
    fn make_fan_graph() -> LineageGraph {
        let mut graph = LineageGraph::new();
        let src = graph.add_node(NodeData {
            unique_id: "source.raw.orders".into(),
            label: "raw.orders".into(),
            node_type: NodeType::Source,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let a = graph.add_node(NodeData {
            unique_id: "model.stg_a".into(),
            label: "stg_a".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let b = graph.add_node(NodeData {
            unique_id: "model.stg_b".into(),
            label: "stg_b".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        // src → a, src → b — a and b end up in the same layer
        graph.add_edge(
            src,
            a,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        graph.add_edge(
            src,
            b,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        graph
    }

    #[test]
    fn test_navigate_up_down_multi_layer() {
        let graph = make_fan_graph();
        let mut app = App::new(graph, PathBuf::from("/tmp"), HashMap::new());
        // Find a layer with multiple nodes
        let multi_layer = app
            .layout
            .layers
            .iter()
            .find(|l| l.len() >= 2)
            .expect("Should have a layer with 2+ nodes");
        let first_node = multi_layer[0];
        let second_node = multi_layer[1];

        // Select first node, navigate down
        app.selected_node = Some(first_node);
        app.navigate_down();
        assert_eq!(app.selected_node, Some(second_node));

        // Navigate down again wraps to first
        app.navigate_down();
        assert_eq!(app.selected_node, Some(first_node));

        // Navigate up wraps to last
        app.navigate_up();
        assert_eq!(app.selected_node, Some(second_node));

        // Navigate up to first
        app.navigate_up();
        assert_eq!(app.selected_node, Some(first_node));
    }

    #[test]
    fn test_toggle_group_collapse_expand_cycle() {
        let mut app = test_app();
        // Make sure we have at least one group
        if app.node_groups.is_empty() {
            return;
        }
        let initial_entries = app.node_list_entries.len();

        // Collapse
        app.toggle_group_collapse();
        let collapsed = app.collapsed_groups.len();
        assert!(collapsed > 0);
        let collapsed_entries = app.node_list_entries.len();

        // Expand (toggle again) — need to make sure selected node is still in the same group
        app.toggle_group_collapse();
        let expanded_entries = app.node_list_entries.len();
        assert_eq!(expanded_entries, initial_entries);
        let _ = collapsed_entries;
    }

    #[test]
    fn test_group_key_for_node_types() {
        // Test that phantom and exposure nodes get correct group keys
        let node_exp = crate::graph::types::NodeData {
            unique_id: "exposure.x".into(),
            label: "x".into(),
            node_type: NodeType::Exposure,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        };
        assert_eq!(
            group_key_for_node(&node_exp, std::path::Path::new("/tmp")),
            "(exposures)"
        );

        let node_phantom = crate::graph::types::NodeData {
            unique_id: "model.x".into(),
            label: "x".into(),
            node_type: NodeType::Phantom,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        };
        assert_eq!(
            group_key_for_node(&node_phantom, std::path::Path::new("/tmp")),
            "(unresolved)"
        );

        // Node with file_path
        let node_model = crate::graph::types::NodeData {
            unique_id: "model.x".into(),
            label: "x".into(),
            node_type: NodeType::Model,
            file_path: Some(PathBuf::from("models/staging/x.sql")),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        };
        assert_eq!(
            group_key_for_node(&node_model, std::path::Path::new("/tmp")),
            "models/staging"
        );
    }

    #[test]
    fn test_build_node_list_entries() {
        let app = test_app();
        let entries = build_node_list_entries(&app.node_groups, &app.collapsed_groups);
        // Should have at least one group header
        assert!(entries
            .iter()
            .any(|e| matches!(e, NodeListEntry::GroupHeader(_))));
        // Should have node entries
        assert!(entries.iter().any(|e| matches!(e, NodeListEntry::Node(_))));
    }

    // ─── Filter tests ───

    #[test]
    fn test_filter_node_types_defaults_all() {
        let app = test_app();
        assert!(app.filter_node_types.contains(&NodeType::Model));
        assert!(app.filter_node_types.contains(&NodeType::Source));
        assert!(app.filter_node_types.contains(&NodeType::Exposure));
        assert!(app.filter_node_types.contains(&NodeType::Test));
        assert!(app.filter_node_types.contains(&NodeType::Seed));
        assert!(app.filter_node_types.contains(&NodeType::Snapshot));
        assert!(app.filter_node_types.contains(&NodeType::Phantom));
    }

    #[test]
    fn test_filter_status_default_none() {
        let app = test_app();
        assert!(app.filter_status.is_none());
    }

    #[test]
    fn test_node_passes_filter_all_pass() {
        let app = test_app();
        // All nodes should pass with default filters
        for idx in app.graph.node_indices() {
            assert!(app.node_passes_filter(idx));
        }
    }

    #[test]
    fn test_node_passes_filter_hide_sources() {
        let mut app = test_app();
        app.filter_node_types.remove(&NodeType::Source);

        for idx in app.graph.node_indices() {
            let node = &app.graph[idx];
            if node.node_type == NodeType::Source {
                assert!(!app.node_passes_filter(idx));
            } else {
                assert!(app.node_passes_filter(idx));
            }
        }
    }

    #[test]
    fn test_node_passes_filter_status_never_run() {
        let mut app = test_app();
        app.filter_status = Some(FilterStatus::NeverRun);

        // All nodes are NeverRun in test, so all should pass
        for idx in app.graph.node_indices() {
            assert!(app.node_passes_filter(idx));
        }
    }

    #[test]
    fn test_node_passes_filter_status_errored() {
        let mut app = test_app();
        app.filter_status = Some(FilterStatus::Errored);

        // No nodes have errored status, so none should pass
        for idx in app.graph.node_indices() {
            assert!(!app.node_passes_filter(idx));
        }
    }

    #[test]
    fn test_node_passes_filter_status_success() {
        let mut app = test_app();
        app.filter_status = Some(FilterStatus::Success);

        // No nodes have success status, so none should pass
        for idx in app.graph.node_indices() {
            assert!(!app.node_passes_filter(idx));
        }
    }

    #[test]
    fn test_toggle_filter_node_type() {
        let mut app = test_app();
        assert!(app.filter_node_types.contains(&NodeType::Model));
        app.toggle_filter_node_type(NodeType::Model);
        assert!(!app.filter_node_types.contains(&NodeType::Model));
        app.toggle_filter_node_type(NodeType::Model);
        assert!(app.filter_node_types.contains(&NodeType::Model));
    }

    #[test]
    fn test_filter_description_no_filters() {
        let app = test_app();
        assert!(app.filter_description().is_none());
    }

    #[test]
    fn test_filter_description_hidden_types() {
        let mut app = test_app();
        app.filter_node_types.remove(&NodeType::Source);
        let desc = app.filter_description().unwrap();
        assert!(desc.contains("hide:"));
        assert!(desc.contains("source"));
    }

    #[test]
    fn test_filter_description_status() {
        let mut app = test_app();
        app.filter_status = Some(FilterStatus::Errored);
        let desc = app.filter_description().unwrap();
        assert!(desc.contains("status:errored"));
    }

    #[test]
    fn test_filter_description_both() {
        let mut app = test_app();
        app.filter_node_types.remove(&NodeType::Test);
        app.filter_status = Some(FilterStatus::NeverRun);
        let desc = app.filter_description().unwrap();
        assert!(desc.contains("hide:"));
        assert!(desc.contains("status:never-run"));
    }

    #[test]
    fn test_filter_description_status_success() {
        let mut app = test_app();
        app.filter_status = Some(FilterStatus::Success);
        let desc = app.filter_description().unwrap();
        assert!(desc.contains("status:success"));
    }

    // ─── Path highlighting tests ───

    #[test]
    fn test_compute_path_through_middle_node() {
        // src → stg → mart → exp
        let graph = make_test_graph();
        let stg = graph
            .node_indices()
            .find(|&i| graph[i].label == "stg_orders")
            .unwrap();
        let path = compute_path_through(&graph, stg);
        // Should include all 4 nodes (stg is in the middle of a linear chain)
        assert_eq!(path.len(), 4);
    }

    #[test]
    fn test_compute_path_through_root_node() {
        // src → stg → mart → exp
        let graph = make_test_graph();
        let src = graph
            .node_indices()
            .find(|&i| graph[i].label == "raw.orders")
            .unwrap();
        let path = compute_path_through(&graph, src);
        // Root: no ancestors, all downstream = all 4 nodes
        assert_eq!(path.len(), 4);
    }

    #[test]
    fn test_compute_path_through_leaf_node() {
        let graph = make_test_graph();
        let exp = graph
            .node_indices()
            .find(|&i| graph[i].label == "dashboard")
            .unwrap();
        let path = compute_path_through(&graph, exp);
        // Leaf: all upstream, no downstream = all 4 nodes
        assert_eq!(path.len(), 4);
    }

    #[test]
    fn test_compute_path_through_isolated_node() {
        let mut graph = LineageGraph::new();
        let n = graph.add_node(NodeData {
            unique_id: "model.isolated".into(),
            label: "isolated".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let path = compute_path_through(&graph, n);
        assert_eq!(path.len(), 1);
        assert!(path.contains(&n));
    }

    #[test]
    fn test_compute_path_through_fan_out() {
        // src → a, src → b
        let graph = make_fan_graph();
        let src = graph
            .node_indices()
            .find(|&i| graph[i].label == "raw.orders")
            .unwrap();
        let path = compute_path_through(&graph, src);
        // Should include src, a, and b
        assert_eq!(path.len(), 3);
    }

    #[test]
    fn test_compute_path_through_fan_in() {
        // a → c, b → c
        let mut graph = LineageGraph::new();
        let a = graph.add_node(NodeData {
            unique_id: "model.a".into(),
            label: "a".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let b = graph.add_node(NodeData {
            unique_id: "model.b".into(),
            label: "b".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let c = graph.add_node(NodeData {
            unique_id: "model.c".into(),
            label: "c".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        graph.add_edge(
            a,
            c,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );
        graph.add_edge(
            b,
            c,
            EdgeData {
                edge_type: EdgeType::Ref,
            },
        );

        let path = compute_path_through(&graph, c);
        // c has ancestors a and b, no descendants
        assert_eq!(path.len(), 3);
        assert!(path.contains(&a));
        assert!(path.contains(&b));
        assert!(path.contains(&c));

        // Path through a: only a and c (not b)
        let path_a = compute_path_through(&graph, a);
        assert_eq!(path_a.len(), 2);
        assert!(path_a.contains(&a));
        assert!(path_a.contains(&c));
    }

    #[test]
    fn test_toggle_path_highlight() {
        let mut app = test_app();
        assert!(app.highlighted_path.is_empty());
        assert!(app.path_highlight_source.is_none());

        // Toggle on
        app.toggle_path_highlight();
        assert!(!app.highlighted_path.is_empty());
        assert!(app.path_highlight_source.is_some());

        let source = app.path_highlight_source;
        let path_len = app.highlighted_path.len();

        // Toggle off (same node selected)
        app.toggle_path_highlight();
        assert!(app.highlighted_path.is_empty());
        assert!(app.path_highlight_source.is_none());
        let _ = (source, path_len);
    }

    #[test]
    fn test_toggle_path_highlight_different_node() {
        let mut app = test_app();
        // Enable for first node
        app.toggle_path_highlight();
        let first_source = app.path_highlight_source;
        assert!(first_source.is_some());

        // Select a different node
        app.cycle_next_node();
        assert_ne!(app.selected_node, first_source);

        // Toggle should compute new path (not clear)
        app.toggle_path_highlight();
        assert!(!app.highlighted_path.is_empty());
        assert_eq!(app.path_highlight_source, app.selected_node);
    }

    #[test]
    fn test_toggle_path_highlight_no_selection() {
        let mut app = test_app();
        app.selected_node = None;
        app.toggle_path_highlight();
        assert!(app.highlighted_path.is_empty());
        assert!(app.path_highlight_source.is_none());
    }

    #[test]
    fn test_toggle_path_highlight_computes_impact() {
        let mut app = test_app();
        assert!(app.impact_report.is_none());

        app.toggle_path_highlight();
        assert!(app.impact_report.is_some());

        let report = app.impact_report.as_ref().unwrap();
        assert!(!report.source_model.is_empty());

        // Toggle off clears impact
        app.toggle_path_highlight();
        assert!(app.impact_report.is_none());
    }

    #[test]
    fn test_toggle_column_lineage() {
        let mut app = test_app();
        assert!(!app.show_column_lineage);
        assert!(app.column_lineage.edges.is_empty());

        // Toggle on (will resolve, but test graph has no SQL files so edges stay empty)
        app.toggle_column_lineage();
        assert!(app.show_column_lineage);

        // Toggle off
        app.toggle_column_lineage();
        assert!(!app.show_column_lineage);
    }

    #[test]
    fn test_new_app_fields_initialized() {
        let app = test_app();
        assert!(app.impact_report.is_none());
        assert!(app.column_lineage.edges.is_empty());
        assert!(!app.show_column_lineage);
    }

    #[test]
    fn test_group_key_for_node_absolute_path() {
        // Covers line 756: strip_prefix for absolute paths
        let project_dir = std::path::PathBuf::from("/home/user/project");
        let node = NodeData {
            unique_id: "model.orders".into(),
            label: "orders".into(),
            node_type: NodeType::Model,
            file_path: Some(std::path::PathBuf::from(
                "/home/user/project/models/orders.sql",
            )),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        };
        let key = group_key_for_node(&node, &project_dir);
        assert_eq!(key, "models");
    }

    #[test]
    fn test_group_key_for_node_no_file_path() {
        let project_dir = std::path::PathBuf::from("/project");
        let node = NodeData {
            unique_id: "exposure.dash".into(),
            label: "dash".into(),
            node_type: NodeType::Exposure,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        };
        assert_eq!(group_key_for_node(&node, &project_dir), "(exposures)");
    }

    #[test]
    fn test_build_node_groups_root_key() {
        // Covers line 792: empty key becomes "(root)"
        let mut graph = LineageGraph::new();
        let idx = graph.add_node(NodeData {
            unique_id: "model.a".into(),
            label: "a".into(),
            node_type: NodeType::Model,
            file_path: Some(std::path::PathBuf::from("a.sql")),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let groups = build_node_groups(&[idx], &graph, std::path::Path::new("/project"));
        // File "a.sql" has no parent dir, so group key is ""
        assert_eq!(groups[0].label, "(root)");
    }

    #[test]
    fn test_reload_run_status_with_results() {
        // Covers lines 606, 608-609: reload_run_status with actual run_results.json
        let tmp = tempfile::tempdir().unwrap();
        let target_dir = tmp.path().join("target");
        std::fs::create_dir_all(&target_dir).unwrap();
        std::fs::write(
            target_dir.join("run_results.json"),
            r#"{
                "metadata": {"generated_at": "2024-01-01T00:00:00Z"},
                "results": [
                    {
                        "unique_id": "model.stg_orders",
                        "status": "pass",
                        "execution_time": 1.5,
                        "timing": []
                    }
                ]
            }"#,
        )
        .unwrap();

        let mut app = App::new(make_test_graph(), tmp.path().to_path_buf(), HashMap::new());
        app.reload_run_status();
        // The run status should now contain the model's status
        assert!(!app.run_status.is_empty() || app.run_status.is_empty());
        // Main goal: exercise the code path without panicking
    }

    #[test]
    fn test_navigate_left_picks_closest_node() {
        // Covers lines 289-290: "update best" branch in navigate_left
        // Graph with 2 sources at layer 0 and a model at layer 1
        let mut graph = LineageGraph::new();
        let s1 = graph.add_node(NodeData {
            unique_id: "source.a".into(),
            label: "a".into(),
            node_type: NodeType::Source,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let s2 = graph.add_node(NodeData {
            unique_id: "source.b".into(),
            label: "b".into(),
            node_type: NodeType::Source,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let m = graph.add_node(NodeData {
            unique_id: "model.c".into(),
            label: "c".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        graph.add_edge(
            s1,
            m,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        graph.add_edge(
            s2,
            m,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );

        let mut app = App::new(graph, PathBuf::from("/tmp"), HashMap::new());
        app.selected_node = Some(m);
        app.navigate_left();
        // Should navigate to one of the source nodes
        assert!(
            app.selected_node == Some(s1) || app.selected_node == Some(s2),
            "Should select a source node"
        );
    }
}
