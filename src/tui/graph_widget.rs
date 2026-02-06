use petgraph::stable_graph::NodeIndex;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use ratatui::buffer::Buffer;
use ratatui::layout::{Position, Rect};
use ratatui::style::{Color, Style};
use ratatui::widgets::Widget;

use crate::graph::types::*;
use crate::parser::artifacts::RunStatus;

use super::app::App;
use super::run_status::{status_color, status_symbol};

/// Node box dimensions in terminal cells
const NODE_BOX_WIDTH: u16 = 24;
const NODE_BOX_HEIGHT: u16 = 3;
const LAYER_GAP: u16 = 12;
const NODE_GAP: u16 = 2;

pub struct GraphWidget<'a> {
    app: &'a App,
}

impl<'a> GraphWidget<'a> {
    pub fn new(app: &'a App) -> Self {
        Self { app }
    }

    /// Compute effective layer gap after zoom
    fn eff_layer_gap(&self) -> u16 {
        (LAYER_GAP as f64 * self.app.zoom).max(4.0) as u16
    }

    /// Compute effective node gap after zoom
    fn eff_node_gap(&self) -> u16 {
        (NODE_GAP as f64 * self.app.zoom).max(1.0) as u16
    }

    /// Convert layout (layer, pos) to world-space (wx, wy) in terminal cells
    fn world_pos(&self, layer: usize, pos: usize) -> (i32, i32) {
        let eff_lg = self.eff_layer_gap();
        let eff_ng = self.eff_node_gap();
        let wx = layer as i32 * (NODE_BOX_WIDTH as i32 + eff_lg as i32);
        let wy = pos as i32 * (NODE_BOX_HEIGHT as i32 + eff_ng as i32);
        (wx, wy)
    }

    /// Convert world-space to screen-space, returning None if outside render area
    fn to_screen(&self, wx: i32, wy: i32, area: Rect) -> Option<(u16, u16)> {
        let sx = wx - self.app.viewport_x + area.x as i32;
        let sy = wy - self.app.viewport_y + area.y as i32;
        if sx >= area.x as i32
            && sy >= area.y as i32
            && sx < (area.x + area.width) as i32
            && sy < (area.y + area.height) as i32
        {
            Some((sx as u16, sy as u16))
        } else {
            None
        }
    }

    /// Set a cell in the buffer if it's within the render area
    fn set_cell(&self, buf: &mut Buffer, wx: i32, wy: i32, area: Rect, symbol: &str, style: Style) {
        if let Some((sx, sy)) = self.to_screen(wx, wy, area) {
            if let Some(cell) = buf.cell_mut(Position::new(sx, sy)) {
                cell.set_symbol(symbol);
                cell.set_style(style);
            }
        }
    }

    /// Draw a horizontal run of a character
    fn draw_hline(&self, buf: &mut Buffer, wx_start: i32, wx_end: i32, wy: i32, area: Rect, symbol: &str, style: Style) {
        let (left, right) = if wx_start <= wx_end {
            (wx_start, wx_end)
        } else {
            (wx_end, wx_start)
        };
        for wx in left..=right {
            self.set_cell(buf, wx, wy, area, symbol, style);
        }
    }

    /// Draw a vertical run of a character
    fn draw_vline(&self, buf: &mut Buffer, wx: i32, wy_start: i32, wy_end: i32, area: Rect, symbol: &str, style: Style) {
        let (top, bottom) = if wy_start <= wy_end {
            (wy_start, wy_end)
        } else {
            (wy_end, wy_start)
        };
        for wy in top..=bottom {
            self.set_cell(buf, wx, wy, area, symbol, style);
        }
    }

    fn draw_edges(&self, buf: &mut Buffer, area: Rect) {
        for edge in self.app.graph.edge_references() {
            let source = edge.source();
            let target = edge.target();

            let (Some(&(sl, sp)), Some(&(tl, tp))) = (
                self.app.layout.positions.get(&source),
                self.app.layout.positions.get(&target),
            ) else {
                continue;
            };

            let color = match edge.weight().edge_type {
                EdgeType::Ref => Color::Gray,
                EdgeType::Source => Color::DarkGray,
                EdgeType::Test => Color::Cyan,
                EdgeType::Exposure => Color::Red,
            };
            let style = Style::default().fg(color);

            let (src_wx, src_wy) = self.world_pos(sl, sp);
            let (tgt_wx, tgt_wy) = self.world_pos(tl, tp);

            // Source right edge midpoint, target left edge midpoint
            let src_right = src_wx + NODE_BOX_WIDTH as i32;
            let src_mid_y = src_wy + NODE_BOX_HEIGHT as i32 / 2;
            let tgt_left = tgt_wx;
            let tgt_mid_y = tgt_wy + NODE_BOX_HEIGHT as i32 / 2;

            // Midpoint column for the vertical segment
            let mid_x = (src_right + tgt_left) / 2;

            if src_mid_y == tgt_mid_y {
                // Same row: straight horizontal line
                self.draw_hline(buf, src_right, tgt_left - 1, src_mid_y, area, "─", style);
                // Arrowhead
                self.set_cell(buf, tgt_left - 1, tgt_mid_y, area, "▸", style);
            } else {
                // Orthogonal 3-segment routing
                // Segment 1: horizontal from source right to midpoint
                if mid_x > src_right {
                    self.draw_hline(buf, src_right, mid_x - 1, src_mid_y, area, "─", style);
                }

                // Segment 2: vertical from source row to target row at midpoint
                let (vy_start, vy_end) = if src_mid_y < tgt_mid_y {
                    (src_mid_y + 1, tgt_mid_y - 1)
                } else {
                    (tgt_mid_y + 1, src_mid_y - 1)
                };
                if vy_start <= vy_end {
                    self.draw_vline(buf, mid_x, vy_start, vy_end, area, "│", style);
                }

                // Segment 3: horizontal from midpoint to target left
                if tgt_left - 1 > mid_x {
                    self.draw_hline(buf, mid_x + 1, tgt_left - 2, tgt_mid_y, area, "─", style);
                }
                // Arrowhead
                self.set_cell(buf, tgt_left - 1, tgt_mid_y, area, "▸", style);

                // Corner characters
                if src_mid_y < tgt_mid_y {
                    // Source above target: ┐ at top-right, └ at bottom-left
                    self.set_cell(buf, mid_x, src_mid_y, area, "┐", style);
                    self.set_cell(buf, mid_x, tgt_mid_y, area, "└", style);
                } else {
                    // Source below target: ┘ at bottom-right, ┌ at top-left
                    self.set_cell(buf, mid_x, src_mid_y, area, "┘", style);
                    self.set_cell(buf, mid_x, tgt_mid_y, area, "┌", style);
                }
            }
        }
    }

    fn draw_nodes(&self, buf: &mut Buffer, area: Rect) {
        for idx in self.app.graph.node_indices() {
            let Some(&(layer, pos)) = self.app.layout.positions.get(&idx) else {
                continue;
            };

            let (wx, wy) = self.world_pos(layer, pos);
            let node = &self.app.graph[idx];
            let is_selected = self.app.selected_node == Some(idx);
            let run_status = self.app.node_run_status(&node.unique_id);

            let node_fg = match run_status {
                RunStatus::NeverRun => node_color(node.node_type),
                _ => status_color(run_status),
            };

            let (border_style, content_style) = if is_selected {
                (
                    Style::default().fg(Color::Black).bg(Color::White),
                    Style::default().fg(Color::Black).bg(Color::White),
                )
            } else {
                (
                    Style::default().fg(node_fg),
                    Style::default().fg(node_fg),
                )
            };

            let w = NODE_BOX_WIDTH as i32;
            let h = NODE_BOX_HEIGHT as i32;

            // Row 0: top border ┌──...──┐
            self.set_cell(buf, wx, wy, area, "┌", border_style);
            for dx in 1..w - 1 {
                self.set_cell(buf, wx + dx, wy, area, "─", border_style);
            }
            self.set_cell(buf, wx + w - 1, wy, area, "┐", border_style);

            // Row 1..h-2: content rows with side borders
            for dy in 1..h - 1 {
                self.set_cell(buf, wx, wy + dy, area, "│", border_style);
                // Fill content area with spaces (for background color)
                for dx in 1..w - 1 {
                    self.set_cell(buf, wx + dx, wy + dy, area, " ", content_style);
                }
                self.set_cell(buf, wx + w - 1, wy + dy, area, "│", border_style);
            }

            // Row h-1: bottom border └──...──┘
            self.set_cell(buf, wx, wy + h - 1, area, "└", border_style);
            for dx in 1..w - 1 {
                self.set_cell(buf, wx + dx, wy + h - 1, area, "─", border_style);
            }
            self.set_cell(buf, wx + w - 1, wy + h - 1, area, "┘", border_style);

            // Label on the content row (row 1)
            let sym = status_symbol(run_status);
            let display = node.display_name();
            let label = format!("{} {}", sym, display);
            let max_chars = (NODE_BOX_WIDTH - 2) as usize; // space inside borders
            let truncated = truncate_label(&label, max_chars);

            // Pad with spaces to fill the box width
            let padded = format!(" {:<width$}", truncated, width = max_chars - 1);

            let content_y = wy + 1;
            for (i, ch) in padded.chars().enumerate() {
                let cx = wx + 1 + i as i32;
                if cx >= wx + w - 1 {
                    break;
                }
                self.set_cell(buf, cx, content_y, area, &ch.to_string(), content_style);
            }
        }
    }
}

impl<'a> Widget for GraphWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.width == 0 || area.height == 0 {
            return;
        }

        // Draw edges first (behind nodes)
        self.draw_edges(buf, area);

        // Draw nodes on top
        self.draw_nodes(buf, area);
    }
}

fn node_color(node_type: NodeType) -> Color {
    match node_type {
        NodeType::Model => Color::Blue,
        NodeType::Source => Color::Green,
        NodeType::Seed => Color::Yellow,
        NodeType::Snapshot => Color::Magenta,
        NodeType::Test => Color::Cyan,
        NodeType::Exposure => Color::Red,
        NodeType::Phantom => Color::DarkGray,
    }
}

fn truncate_label(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}…", &s[..max_len - 1])
    }
}

/// Hit-test a screen coordinate against all node boxes.
/// Returns the NodeIndex of the first node whose bounding box contains the point.
pub fn hit_test_node(app: &App, screen_x: u16, screen_y: u16) -> Option<NodeIndex> {
    let area = app.last_graph_area?;

    let eff_lg = (LAYER_GAP as f64 * app.zoom).max(4.0) as u16;
    let eff_ng = (NODE_GAP as f64 * app.zoom).max(1.0) as u16;

    // Convert screen coords to world coords
    let wx = (screen_x as i32 - area.x as i32) + app.viewport_x;
    let wy = (screen_y as i32 - area.y as i32) + app.viewport_y;

    for (&node_idx, &(layer, pos)) in &app.layout.positions {
        let node_wx = layer as i32 * (NODE_BOX_WIDTH as i32 + eff_lg as i32);
        let node_wy = pos as i32 * (NODE_BOX_HEIGHT as i32 + eff_ng as i32);

        if wx >= node_wx
            && wx < node_wx + NODE_BOX_WIDTH as i32
            && wy >= node_wy
            && wy < node_wy + NODE_BOX_HEIGHT as i32
        {
            return Some(node_idx);
        }
    }
    None
}

/// Compute world-space center of a node given its layout position.
/// Used by App::center_on_selected.
pub fn node_world_center(layer: usize, pos: usize, zoom: f64) -> (i32, i32) {
    let eff_lg = (LAYER_GAP as f64 * zoom).max(4.0) as u16;
    let eff_ng = (NODE_GAP as f64 * zoom).max(1.0) as u16;
    let wx = layer as i32 * (NODE_BOX_WIDTH as i32 + eff_lg as i32);
    let wy = pos as i32 * (NODE_BOX_HEIGHT as i32 + eff_ng as i32);
    let cx = wx + NODE_BOX_WIDTH as i32 / 2;
    let cy = wy + NODE_BOX_HEIGHT as i32 / 2;
    (cx, cy)
}
