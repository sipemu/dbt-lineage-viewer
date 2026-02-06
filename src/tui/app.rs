use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use indexmap::IndexMap;
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use ratatui::layout::Rect;
use ratatui::widgets::ListState;

use crate::graph::types::{LineageGraph, NodeType};
use crate::parser::artifacts::{self, RunStatus, RunStatusMap};
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
    #[allow(dead_code)]
    pub should_quit: bool,

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
            should_quit: false,
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
        let Some(current) = self.selected_node else { return };
        let Some(&(cur_layer, cur_pos)) = self.layout.positions.get(&current) else { return };

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
        let Some(current) = self.selected_node else { return };
        let Some(&(cur_layer, cur_pos)) = self.layout.positions.get(&current) else { return };
        if cur_layer == 0 { return; }

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
        let Some(current) = self.selected_node else { return };
        let Some(&(cur_layer, _cur_pos)) = self.layout.positions.get(&current) else { return };

        if cur_layer >= self.layout.layers.len() { return; }
        let layer = &self.layout.layers[cur_layer];
        if layer.len() <= 1 { return; }

        // Find current position in the layer vec
        let Some(idx) = layer.iter().position(|&n| n == current) else { return };
        let new_idx = if idx == 0 { layer.len() - 1 } else { idx - 1 };

        self.selected_node = Some(layer[new_idx]);
        self.sync_cycle_index();
        self.sync_node_list_state();
        self.center_on_selected();
    }

    /// Navigate down within the same layer (wraps around)
    pub fn navigate_down(&mut self) {
        let Some(current) = self.selected_node else { return };
        let Some(&(cur_layer, _cur_pos)) = self.layout.positions.get(&current) else { return };

        if cur_layer >= self.layout.layers.len() { return; }
        let layer = &self.layout.layers[cur_layer];
        if layer.len() <= 1 { return; }

        let Some(idx) = layer.iter().position(|&n| n == current) else { return };
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
        let Some(selected) = self.selected_node else { return };

        // Auto-expand the group containing the selected node
        let group_key = self.group_key_for_selected(selected);
        if let Some(key) = group_key {
            if self.collapsed_groups.remove(&key) {
                self.node_list_entries =
                    build_node_list_entries(&self.node_groups, &self.collapsed_groups);
            }
        }

        // Find flat index of this node in node_list_entries
        if let Some(flat_idx) = self.node_list_entries.iter().position(|e| {
            matches!(e, NodeListEntry::Node(idx) if *idx == selected)
        }) {
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
        let Some(selected) = self.selected_node else { return };

        // Find which group the selected node belongs to
        let group_idx = match self.node_groups.iter().position(|g| g.nodes.contains(&selected)) {
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
            if let Some(flat_idx) = self.node_list_entries.iter().position(|e| {
                matches!(e, NodeListEntry::Node(idx) if *idx == selected)
            }) {
                self.node_list_state.select(Some(flat_idx));
            }
        } else {
            // Collapse: add to set, rebuild, select the group header row
            self.collapsed_groups.insert(key);
            self.node_list_entries =
                build_node_list_entries(&self.node_groups, &self.collapsed_groups);
            // Select the group header row
            if let Some(flat_idx) = self.node_list_entries.iter().position(|e| {
                matches!(e, NodeListEntry::GroupHeader(i) if *i == group_idx)
            }) {
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
        self.node_list_entries =
            build_node_list_entries(&self.node_groups, &self.collapsed_groups);
    }

    /// Center the viewport on the currently selected node
    pub fn center_on_selected(&mut self) {
        let Some(selected) = self.selected_node else { return };
        let Some(&(layer, pos)) = self.layout.positions.get(&selected) else { return };

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
            path.strip_prefix(project_dir)
                .unwrap_or(path.as_path())
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
            let label = if key.is_empty() { "(root)".to_string() } else { key.clone() };
            NodeGroup { key, label, nodes }
        })
        .collect()
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
