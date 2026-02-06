use ratatui::style::Color;

use crate::parser::artifacts::RunStatus;

/// Get the display symbol for a run status
pub fn status_symbol(status: &RunStatus) -> &'static str {
    match status {
        RunStatus::NeverRun => "?",
        RunStatus::Success { .. } => "\u{2713}", // ✓
        RunStatus::Error { .. } => "\u{2717}",   // ✗
        RunStatus::Skipped { .. } => "-",
        RunStatus::Outdated { .. } => "~",
    }
}

/// Get a human-readable label for a run status
pub fn status_label(status: &RunStatus) -> String {
    match status {
        RunStatus::NeverRun => "Never run".to_string(),
        RunStatus::Success { completed_at } => {
            format!("Success ({})", completed_at.format("%Y-%m-%d %H:%M:%S"))
        }
        RunStatus::Error { message, .. } => {
            format!("Error: {}", message)
        }
        RunStatus::Skipped { .. } => "Skipped".to_string(),
        RunStatus::Outdated { run_at, .. } => {
            format!("Outdated (ran {})", run_at.format("%Y-%m-%d %H:%M:%S"))
        }
    }
}

/// Get the ratatui color for a run status
pub fn status_color(status: &RunStatus) -> Color {
    match status {
        RunStatus::NeverRun => Color::DarkGray,
        RunStatus::Success { .. } => Color::Green,
        RunStatus::Error { .. } => Color::Red,
        RunStatus::Skipped { .. } => Color::DarkGray,
        RunStatus::Outdated { .. } => Color::Yellow,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_status_symbols() {
        assert_eq!(status_symbol(&RunStatus::NeverRun), "?");
        assert_eq!(
            status_symbol(&RunStatus::Success {
                completed_at: Utc::now()
            }),
            "\u{2713}"
        );
        assert_eq!(
            status_symbol(&RunStatus::Error {
                completed_at: None,
                message: "err".to_string()
            }),
            "\u{2717}"
        );
        assert_eq!(
            status_symbol(&RunStatus::Skipped {
                completed_at: None
            }),
            "-"
        );
    }

    #[test]
    fn test_status_colors() {
        assert_eq!(status_color(&RunStatus::NeverRun), Color::DarkGray);
        assert_eq!(
            status_color(&RunStatus::Success {
                completed_at: Utc::now()
            }),
            Color::Green
        );
        assert_eq!(
            status_color(&RunStatus::Error {
                completed_at: None,
                message: "err".to_string()
            }),
            Color::Red
        );
    }
}
