use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;

/// Which dbt command to run
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbtCommand {
    Run,
    Test,
}

impl DbtCommand {
    pub fn as_str(&self) -> &'static str {
        match self {
            DbtCommand::Run => "run",
            DbtCommand::Test => "test",
        }
    }
}

/// Scope of model selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionScope {
    /// Just this model
    Single,
    /// +model (upstream included)
    WithUpstream,
    /// model+ (downstream included)
    WithDownstream,
    /// +model+ (full lineage)
    FullLineage,
}

impl SelectionScope {
    pub fn format_selector(&self, model_name: &str) -> String {
        match self {
            SelectionScope::Single => model_name.to_string(),
            SelectionScope::WithUpstream => format!("+{}", model_name),
            SelectionScope::WithDownstream => format!("{}+", model_name),
            SelectionScope::FullLineage => format!("+{}+", model_name),
        }
    }

    #[allow(dead_code)]
    pub fn label(&self) -> &'static str {
        match self {
            SelectionScope::Single => "this model",
            SelectionScope::WithUpstream => "+upstream",
            SelectionScope::WithDownstream => "downstream+",
            SelectionScope::FullLineage => "+full lineage+",
        }
    }
}

/// Detect whether to use `uv run dbt` or plain `dbt`.
///
/// Returns true if:
/// - `uv.lock` or `pyproject.toml` exists in the project directory, OR
/// - `dbt` is not found on PATH but `uv` is available (fallback)
pub fn detect_use_uv(project_dir: &Path) -> bool {
    if project_dir.join("uv.lock").exists() || project_dir.join("pyproject.toml").exists() {
        return true;
    }
    // Fallback: if `dbt` isn't on PATH, try `uv run dbt` as a last resort
    let dbt_on_path = Command::new("dbt")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .is_ok();
    if !dbt_on_path {
        // Check if uv is available
        let uv_on_path = Command::new("uv")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .is_ok();
        if uv_on_path {
            return true;
        }
    }
    false
}

/// A request to run a dbt command
#[derive(Debug, Clone)]
pub struct DbtRunRequest {
    pub command: DbtCommand,
    pub scope: SelectionScope,
    pub model_name: String,
    pub project_dir: PathBuf,
    pub use_uv: bool,
}

impl DbtRunRequest {
    /// The program to invoke (either "uv" or "dbt")
    pub fn program(&self) -> &'static str {
        if self.use_uv { "uv" } else { "dbt" }
    }

    /// Build the full argument list for the command
    pub fn args(&self) -> Vec<String> {
        let selector = self.scope.format_selector(&self.model_name);
        let mut args = Vec::new();
        if self.use_uv {
            args.push("run".to_string());
            args.push("dbt".to_string());
        }
        args.push(self.command.as_str().to_string());
        args.push("--select".to_string());
        args.push(selector);
        args.push("--project-dir".to_string());
        args.push(self.project_dir.display().to_string());
        args
    }

    /// Human-readable command string for display
    pub fn display_command(&self) -> String {
        let args = self.args();
        format!("{} {}", self.program(), args.join(" "))
    }
}

/// Messages sent from the background dbt process
#[derive(Debug)]
pub enum DbtRunMessage {
    OutputLine(String),
    Completed { success: bool },
    SpawnError(String),
}

/// Spawn a dbt run in a background thread.
/// Returns a receiver for progress messages.
pub fn spawn_dbt_run(request: DbtRunRequest) -> mpsc::Receiver<DbtRunMessage> {
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let program = request.program();
        let args = request.args();
        let result = Command::new(program)
            .args(&args)
            .current_dir(&request.project_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();

        match result {
            Err(e) => {
                let _ = tx.send(DbtRunMessage::SpawnError(format!(
                    "Failed to spawn: `{}`\n  Caused by: {}\n  Hint: ensure dbt is installed and on PATH, or use a uv-managed project (uv.lock / pyproject.toml)",
                    program, e
                )));
            }
            Ok(mut child) => {
                // Read stdout in a thread
                let stdout = child.stdout.take();
                let tx_out = tx.clone();
                let stdout_handle = thread::spawn(move || {
                    if let Some(stdout) = stdout {
                        let reader = BufReader::new(stdout);
                        for line in reader.lines() {
                            match line {
                                Ok(line) => {
                                    if tx_out.send(DbtRunMessage::OutputLine(line)).is_err() {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                });

                // Read stderr in a thread
                let stderr = child.stderr.take();
                let tx_err = tx.clone();
                let stderr_handle = thread::spawn(move || {
                    if let Some(stderr) = stderr {
                        let reader = BufReader::new(stderr);
                        for line in reader.lines() {
                            match line {
                                Ok(line) => {
                                    if tx_err.send(DbtRunMessage::OutputLine(line)).is_err() {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                });

                let _ = stdout_handle.join();
                let _ = stderr_handle.join();

                let success = match child.wait() {
                    Ok(status) => status.success(),
                    Err(_) => false,
                };

                let _ = tx.send(DbtRunMessage::Completed { success });
            }
        }
    });

    rx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_selection_scope_format() {
        assert_eq!(
            SelectionScope::Single.format_selector("stg_orders"),
            "stg_orders"
        );
        assert_eq!(
            SelectionScope::WithUpstream.format_selector("stg_orders"),
            "+stg_orders"
        );
        assert_eq!(
            SelectionScope::WithDownstream.format_selector("stg_orders"),
            "stg_orders+"
        );
        assert_eq!(
            SelectionScope::FullLineage.format_selector("stg_orders"),
            "+stg_orders+"
        );
    }

    #[test]
    fn test_dbt_run_request_args() {
        let req = DbtRunRequest {
            command: DbtCommand::Run,
            scope: SelectionScope::WithUpstream,
            model_name: "stg_orders".to_string(),
            project_dir: PathBuf::from("/tmp/project"),
            use_uv: false,
        };
        let args = req.args();
        assert_eq!(args, vec!["run", "--select", "+stg_orders", "--project-dir", "/tmp/project"]);
    }

    #[test]
    fn test_dbt_run_request_args_uv() {
        let req = DbtRunRequest {
            command: DbtCommand::Run,
            scope: SelectionScope::Single,
            model_name: "orders".to_string(),
            project_dir: PathBuf::from("/tmp/project"),
            use_uv: true,
        };
        let args = req.args();
        assert_eq!(args, vec!["run", "dbt", "run", "--select", "orders", "--project-dir", "/tmp/project"]);
        assert_eq!(req.program(), "uv");
    }

    #[test]
    fn test_display_command() {
        let req = DbtRunRequest {
            command: DbtCommand::Test,
            scope: SelectionScope::Single,
            model_name: "orders".to_string(),
            project_dir: PathBuf::from("/tmp/project"),
            use_uv: false,
        };
        assert_eq!(
            req.display_command(),
            "dbt test --select orders --project-dir /tmp/project"
        );
    }

    #[test]
    fn test_display_command_uv() {
        let req = DbtRunRequest {
            command: DbtCommand::Run,
            scope: SelectionScope::WithUpstream,
            model_name: "stg_orders".to_string(),
            project_dir: PathBuf::from("/tmp/project"),
            use_uv: true,
        };
        assert_eq!(
            req.display_command(),
            "uv run dbt run --select +stg_orders --project-dir /tmp/project"
        );
    }

    #[test]
    fn test_detect_use_uv_with_lock_file() {
        use std::fs;
        let dir = std::env::temp_dir().join("dbt_test_uv_detect_lock");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // With uv.lock → always true
        fs::write(dir.join("uv.lock"), "").unwrap();
        assert!(detect_use_uv(&dir));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_detect_use_uv_with_pyproject() {
        use std::fs;
        let dir = std::env::temp_dir().join("dbt_test_uv_detect_pyproject");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // With pyproject.toml → always true
        fs::write(dir.join("pyproject.toml"), "").unwrap();
        assert!(detect_use_uv(&dir));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_detect_use_uv_fallback() {
        use std::fs;
        let dir = std::env::temp_dir().join("dbt_test_uv_detect_fallback");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // No marker files: result depends on what's on PATH.
        // If dbt is on PATH → false (use dbt directly).
        // If dbt is NOT on PATH but uv IS → true (fallback to uv run dbt).
        // If neither is on PATH → false.
        // We can't assert a fixed value, but we can verify it doesn't panic.
        let _result = detect_use_uv(&dir);

        let _ = fs::remove_dir_all(&dir);
    }
}
