use ratatui::prelude::*;
use ratatui::widgets::*;

use crate::graph::types::*;
use crate::parser::artifacts::RunStatus;

use super::app::{App, AppMode, DbtRunState, NodeListEntry};
use super::graph_widget::GraphWidget;
use super::run_status::{status_color, status_label, status_symbol};

pub fn draw_ui(f: &mut Frame, app: &mut App) {
    // Main layout depends on whether node list panel is visible
    let main_chunks = if app.show_node_list {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(20),
                Constraint::Percentage(50),
                Constraint::Percentage(30),
            ])
            .split(f.area())
    } else {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
            .split(f.area())
    };

    let (graph_area, detail_area) = if app.show_node_list {
        draw_node_list(f, app, main_chunks[0]);
        (main_chunks[1], main_chunks[2])
    } else {
        (main_chunks[0], main_chunks[1])
    };

    // Left: graph + help bar
    let left_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(3)])
        .split(graph_area);

    draw_graph(f, app, left_chunks[0]);
    draw_help_bar(f, app, left_chunks[1]);
    draw_detail_panel(f, app, detail_area);

    // Draw overlays on top
    match app.mode {
        AppMode::RunMenu => draw_run_menu(f, app),
        AppMode::ContextMenu => draw_context_menu(f, app),
        AppMode::RunConfirm => draw_run_confirm(f, app),
        AppMode::RunOutput => draw_run_output(f, app),
        _ => {}
    }
}

fn draw_graph(f: &mut Frame, app: &mut App, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Lineage Graph ");
    let inner = block.inner(area);
    f.render_widget(block, area);
    app.last_graph_area = Some(inner);
    f.render_widget(GraphWidget::new(app), inner);
}

fn draw_node_list(f: &mut Frame, app: &mut App, area: Rect) {
    app.last_node_list_area = Some(area);

    let items: Vec<ListItem> = app
        .node_list_entries
        .iter()
        .map(|entry| match entry {
            NodeListEntry::GroupHeader(gi) => {
                let group = &app.node_groups[*gi];
                let is_collapsed = app.collapsed_groups.contains(&group.key);
                let arrow = if is_collapsed { "\u{25b8}" } else { "\u{25be}" };
                let label = format!("{} {} ({})", arrow, group.label, group.nodes.len());
                ListItem::new(label)
                    .style(Style::default().fg(Color::White).bold())
            }
            NodeListEntry::Node(idx) => {
                let node = &app.graph[*idx];
                let run_status = app.node_run_status(&node.unique_id);
                let sym = status_symbol(run_status);
                let color = status_color(run_status);
                let is_selected = app.selected_node == Some(*idx);

                let style = if is_selected {
                    Style::default().fg(Color::Black).bg(Color::White)
                } else {
                    Style::default().fg(color)
                };

                let display = node.display_name();
                let label = format!("   {} {}", sym, display);
                ListItem::new(label).style(style)
            }
        })
        .collect();

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Nodes "),
        )
        .highlight_style(Style::default().fg(Color::Black).bg(Color::White));

    f.render_stateful_widget(list, area, &mut app.node_list_state);
}

fn draw_detail_panel(f: &mut Frame, app: &App, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Details ");

    let inner = block.inner(area);
    f.render_widget(block, area);

    let Some(selected) = app.selected_node else {
        let text = Paragraph::new("No node selected.\nUse Tab to cycle nodes.");
        f.render_widget(text, inner);
        return;
    };

    let node = &app.graph[selected];
    let run_status = app.node_run_status(&node.unique_id);

    let mut lines = vec![
        Line::from(vec![
            Span::styled("Name: ", Style::default().bold()),
            Span::raw(&node.label),
        ]),
        Line::from(vec![
            Span::styled("Type: ", Style::default().bold()),
            Span::styled(
                node.node_type.label(),
                Style::default().fg(node_color(node.node_type)),
            ),
        ]),
        Line::from(vec![
            Span::styled("ID:   ", Style::default().bold()),
            Span::raw(&node.unique_id),
        ]),
    ];

    if let Some(path) = &node.file_path {
        lines.push(Line::from(vec![
            Span::styled("File: ", Style::default().bold()),
            Span::raw(path.display().to_string()),
        ]));
    }

    // Run status
    lines.push(Line::from(vec![
        Span::styled("Status: ", Style::default().bold()),
        Span::styled(status_label(run_status), Style::default().fg(status_color(run_status))),
    ]));

    // Last run timestamp
    match run_status {
        RunStatus::Success { completed_at } => {
            lines.push(Line::from(vec![
                Span::styled("Last run: ", Style::default().bold()),
                Span::raw(completed_at.format("%Y-%m-%d %H:%M:%S UTC").to_string()),
            ]));
        }
        RunStatus::Error {
            completed_at: Some(ts),
            message,
            ..
        } => {
            lines.push(Line::from(vec![
                Span::styled("Last run: ", Style::default().bold()),
                Span::raw(ts.format("%Y-%m-%d %H:%M:%S UTC").to_string()),
            ]));
            lines.push(Line::from(vec![
                Span::styled("Error: ", Style::default().bold().fg(Color::Red)),
                Span::raw(message.as_str()),
            ]));
        }
        RunStatus::Error {
            completed_at: None,
            message,
            ..
        } => {
            lines.push(Line::from(vec![
                Span::styled("Error: ", Style::default().bold().fg(Color::Red)),
                Span::raw(message.as_str()),
            ]));
        }
        RunStatus::Outdated { run_at, .. } => {
            lines.push(Line::from(vec![
                Span::styled("Last run: ", Style::default().bold()),
                Span::raw(run_at.format("%Y-%m-%d %H:%M:%S UTC").to_string()),
            ]));
        }
        _ => {}
    }

    if let Some(desc) = &node.description {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            "Description:",
            Style::default().bold(),
        )]));
        lines.push(Line::from(desc.as_str()));
    }

    // Upstream
    let upstream = app.upstream_of(selected);
    if !upstream.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            "Upstream:",
            Style::default().bold(),
        )]));
        for up in &upstream {
            let n = &app.graph[*up];
            lines.push(Line::from(format!("  {} ({})", n.label, n.node_type.label())));
        }
    }

    // Downstream
    let downstream = app.downstream_of(selected);
    if !downstream.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![Span::styled(
            "Downstream:",
            Style::default().bold(),
        )]));
        for down in &downstream {
            let n = &app.graph[*down];
            lines.push(Line::from(format!("  {} ({})", n.label, n.node_type.label())));
        }
    }

    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: true });
    f.render_widget(paragraph, inner);
}

fn draw_help_bar(f: &mut Frame, app: &App, area: Rect) {
    let text = match app.mode {
        AppMode::Normal => {
            let mut help = String::from(
                " hjkl/\u{2190}\u{2193}\u{2191}\u{2192}: navigate | HJKL: pan | +/-: zoom | Tab: cycle | /: search | n: nodes | r: reset | x: run",
            );
            if app.show_node_list {
                help.push_str(" | c: collapse");
            }
            if app.has_run_output() {
                help.push_str(" | o: output");
            }
            if app.is_run_in_progress() {
                help.push_str(" | [running...]");
            }
            help.push_str(" | q: quit");
            help
        }
        AppMode::Search => {
            format!(
                " Search: {}_ | Tab: next result | Esc: cancel",
                app.search_query
            )
        }
        AppMode::RunMenu | AppMode::ContextMenu => {
            " r: run | u: +upstream | d: downstream+ | a: +all+ | t: test | Esc: cancel"
                .to_string()
        }
        AppMode::RunConfirm => " y/Enter: execute | n/Esc: cancel".to_string(),
        AppMode::RunOutput => " j/k: scroll | G: bottom | Esc/q: close".to_string(),
    };

    let style = match app.mode {
        AppMode::Normal => Style::default().bg(Color::DarkGray).fg(Color::White),
        AppMode::Search => Style::default().bg(Color::Blue).fg(Color::White),
        AppMode::RunMenu | AppMode::ContextMenu => Style::default().bg(Color::Magenta).fg(Color::White),
        AppMode::RunConfirm => Style::default().bg(Color::Yellow).fg(Color::Black),
        AppMode::RunOutput => Style::default().bg(Color::Cyan).fg(Color::Black),
    };

    let help = Paragraph::new(text).style(style);
    f.render_widget(help, area);
}

fn draw_run_menu(f: &mut Frame, app: &mut App) {
    let area = f.area();
    let popup = centered_rect(42, 14, area);

    app.last_run_menu_area = Some(popup);

    let model_name = app
        .selected_node
        .map(|idx| app.graph[idx].label.as_str())
        .unwrap_or("?");

    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" Run: {} ", model_name))
        .border_style(Style::default().fg(Color::Magenta));

    let hover = app.menu_hover_index;
    let text = vec![
        Line::from(""),
        menu_item_line("  r", "  dbt run (this model)", hover == Some(0)),
        menu_item_line("  u", "  dbt run +upstream", hover == Some(1)),
        menu_item_line("  d", "  dbt run downstream+", hover == Some(2)),
        menu_item_line("  a", "  dbt run +all+", hover == Some(3)),
        menu_item_line("  t", "  dbt test", hover == Some(4)),
        Line::from(""),
        Line::from(Span::styled(
            "  Esc to cancel",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(Clear, popup);
    f.render_widget(paragraph, popup);
}

fn draw_context_menu(f: &mut Frame, app: &mut App) {
    let Some((mx, my)) = app.context_menu_pos else { return };

    let menu_width: u16 = 30;
    let menu_height: u16 = 10;
    let area = f.area();

    // Clamp position so menu stays on screen
    let x = mx.min(area.x + area.width.saturating_sub(menu_width));
    let y = my.min(area.y + area.height.saturating_sub(menu_height));

    let popup = Rect {
        x,
        y,
        width: menu_width.min(area.width),
        height: menu_height.min(area.height),
    };

    app.last_context_menu_area = Some(popup);

    let model_name = app
        .selected_node
        .map(|idx| app.graph[idx].label.as_str())
        .unwrap_or("?");

    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" {} ", model_name))
        .border_style(Style::default().fg(Color::Magenta));

    let hover = app.menu_hover_index;
    let text = vec![
        menu_item_line(" r", "  dbt run", hover == Some(0)),
        menu_item_line(" u", "  dbt run +upstream", hover == Some(1)),
        menu_item_line(" d", "  dbt run downstream+", hover == Some(2)),
        menu_item_line(" a", "  dbt run +all+", hover == Some(3)),
        menu_item_line(" t", "  dbt test", hover == Some(4)),
        Line::from(""),
        Line::from(Span::styled(
            " Esc to close",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(Clear, popup);
    f.render_widget(paragraph, popup);
}

fn draw_run_confirm(f: &mut Frame, app: &mut App) {
    let area = f.area();
    let popup = centered_rect(60, 8, area);

    app.last_confirm_area = Some(popup);

    let command_str = app
        .pending_run
        .as_ref()
        .map(|r| r.display_command())
        .unwrap_or_else(|| "???".to_string());

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Confirm ")
        .border_style(Style::default().fg(Color::Yellow));

    let exec_style = if app.confirm_hover == Some(true) {
        Style::default().bold().fg(Color::Black).bg(Color::Green)
    } else {
        Style::default().bold().fg(Color::Green)
    };
    let cancel_style = if app.confirm_hover == Some(false) {
        Style::default().bold().fg(Color::Black).bg(Color::Red)
    } else {
        Style::default().bold().fg(Color::Red)
    };

    let text = vec![
        Line::from(""),
        Line::from("  Execute this command?"),
        Line::from(""),
        Line::from(Span::styled(
            format!("  $ {}", command_str),
            Style::default().bold().fg(Color::Cyan),
        )),
        Line::from(""),
        Line::from(vec![
            Span::raw("  "),
            Span::styled(" Execute (y) ", exec_style),
            Span::raw("  "),
            Span::styled(" Cancel (n) ", cancel_style),
        ]),
    ];

    let paragraph = Paragraph::new(text).block(block);
    f.render_widget(Clear, popup);
    f.render_widget(paragraph, popup);
}

fn draw_run_output(f: &mut Frame, app: &App) {
    let area = f.area();
    // Full-screen overlay with 2-cell margin
    let popup = Rect {
        x: area.x + 2,
        y: area.y + 1,
        width: area.width.saturating_sub(4),
        height: area.height.saturating_sub(2),
    };

    let (lines, is_running, success) = match &app.run_state {
        DbtRunState::Running { output_lines, .. } => (output_lines, true, false),
        DbtRunState::Finished {
            output_lines,
            success,
        } => (output_lines, false, *success),
        DbtRunState::Idle => return,
    };

    let border_color = if is_running {
        Color::Yellow
    } else if success {
        Color::Green
    } else {
        Color::Red
    };

    let title = if is_running {
        " dbt (running...) "
    } else if success {
        " dbt (success) "
    } else {
        " dbt (failed) "
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(Style::default().fg(border_color));

    let inner = block.inner(popup);
    let visible_height = inner.height as usize;

    // Clamp scroll
    let max_scroll = lines.len().saturating_sub(visible_height);
    let scroll = app.run_output_scroll.min(max_scroll);

    let text_lines: Vec<Line> = lines
        .iter()
        .skip(scroll)
        .take(visible_height)
        .map(|l| Line::from(l.as_str()))
        .collect();

    let paragraph = Paragraph::new(text_lines).block(block);
    f.render_widget(Clear, popup);
    f.render_widget(paragraph, popup);
}

/// Build a single menu item line with optional hover highlight.
fn menu_item_line<'a>(key: &'a str, desc: &'a str, hovered: bool) -> Line<'a> {
    let line = Line::from(vec![
        Span::styled(key, Style::default().bold().fg(Color::Yellow)),
        Span::raw(desc),
    ]);
    if hovered {
        line.style(Style::default().bg(Color::DarkGray))
    } else {
        line
    }
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    Rect {
        x,
        y,
        width: width.min(area.width),
        height: height.min(area.height),
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

