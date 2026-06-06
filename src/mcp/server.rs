//! Minimal JSON-RPC 2.0 stdio server implementing the MCP handshake plus
//! `tools/list` and `tools/call`. Stays sync (no tokio) — one line in, one
//! line out — to match the rest of the project.

use std::io::{BufRead, Write};

use serde_json::{json, Value};

use crate::graph::types::LineageGraph;
use crate::mcp::tools;

/// Server identity sent back in the `initialize` response.
const SERVER_NAME: &str = "dbt-lineage";
const PROTOCOL_VERSION: &str = "2024-11-05";

/// Run the MCP stdio server loop. Reads JSON-RPC requests line-by-line from
/// `input`, writes responses to `output`. Returns Ok when stdin closes cleanly.
pub fn run<R: BufRead, W: Write>(
    graph: LineageGraph,
    mut input: R,
    output: &mut W,
) -> std::io::Result<()> {
    let mut line = String::new();
    loop {
        line.clear();
        let n = input.read_line(&mut line)?;
        if n == 0 {
            // EOF
            return Ok(());
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(req) => handle_message(&graph, &req, output)?,
            Err(e) => {
                // Send parse error (no id known).
                let resp = error_response(&Value::Null, -32700, &format!("parse error: {}", e));
                writeln_json(output, &resp)?;
            }
        }
    }
}

/// Dispatch a single decoded JSON-RPC message. Notifications (no `id`) receive
/// no response; requests get either a success result or an error.
fn handle_message<W: Write>(
    graph: &LineageGraph,
    req: &Value,
    output: &mut W,
) -> std::io::Result<()> {
    let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let is_notification = req.get("id").is_none();

    // Notifications never get a response.
    if is_notification {
        // `notifications/initialized` is the only one we expect; silently accept others.
        return Ok(());
    }

    let response = match method {
        "initialize" => Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": SERVER_NAME, "version": env!("CARGO_PKG_VERSION")},
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(handle_tools_list()),
        "tools/call" => handle_tools_call(graph, req.get("params")),
        other => Err((-32601, format!("method not found: {}", other))),
    };

    let resp = match response {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err((code, msg)) => error_response(&id, code, &msg),
    };
    writeln_json(output, &resp)?;
    Ok(())
}

fn handle_tools_list() -> Value {
    let tools: Vec<Value> = tools::registry()
        .into_iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description,
                "inputSchema": t.input_schema,
            })
        })
        .collect();
    json!({"tools": tools})
}

fn handle_tools_call(graph: &LineageGraph, params: Option<&Value>) -> Result<Value, (i64, String)> {
    let params = params.ok_or_else(|| (-32602, "missing params".into()))?;
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| (-32602, "missing 'name' in params".into()))?;
    let empty = json!({});
    let arguments = params.get("arguments").unwrap_or(&empty);

    match tools::call_tool(graph, name, arguments) {
        Ok(value) => {
            // MCP convention: tool results are wrapped in a `content` array of
            // typed parts. We use a single `text` part holding the JSON payload.
            let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
            Ok(json!({
                "content": [{"type": "text", "text": text}],
                "isError": false,
            }))
        }
        Err(err) => {
            // MCP convention: tool-level errors return content with isError=true,
            // not a JSON-RPC error. Reserves JSON-RPC errors for protocol issues.
            Ok(json!({
                "content": [{"type": "text", "text": err.to_string()}],
                "isError": true,
            }))
        }
    }
}

fn error_response(id: &Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message},
    })
}

fn writeln_json<W: Write>(output: &mut W, value: &Value) -> std::io::Result<()> {
    let s = serde_json::to_string(value).expect("response must serialize");
    writeln!(output, "{}", s)?;
    output.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::*;
    use std::io::Cursor;

    fn make_graph() -> LineageGraph {
        let mut g = LineageGraph::new();
        let s = g.add_node(NodeData {
            unique_id: "source.raw.orders".into(),
            label: "raw.orders".into(),
            node_type: NodeType::Source,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        let m = g.add_node(NodeData {
            unique_id: "model.orders".into(),
            label: "orders".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });
        g.add_edge(
            s,
            m,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        g
    }

    fn run_with(input: &str) -> Vec<Value> {
        let mut out: Vec<u8> = Vec::new();
        run(make_graph(), Cursor::new(input.as_bytes()), &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        text.lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str::<Value>(l).expect("response must be valid JSON"))
            .collect()
    }

    #[test]
    fn test_initialize_returns_server_info() {
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        let responses = run_with(&format!("{}\n", req));
        assert_eq!(responses.len(), 1);
        let r = &responses[0];
        assert_eq!(r["id"], 1);
        assert_eq!(r["result"]["serverInfo"]["name"], SERVER_NAME);
        assert!(r["result"]["protocolVersion"].is_string());
    }

    #[test]
    fn test_notifications_get_no_response() {
        let req = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
        let responses = run_with(&format!("{}\n", req));
        assert!(responses.is_empty());
    }

    #[test]
    fn test_tools_list_returns_all_tools() {
        let req = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#;
        let responses = run_with(&format!("{}\n", req));
        let names: Vec<&str> = responses[0]["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"summary"));
        assert!(names.contains(&"lineage"));
        assert!(names.contains(&"impact"));
        assert!(names.contains(&"search_models"));
    }

    #[test]
    fn test_tools_call_summary_returns_text_content() {
        let req = r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"summary","arguments":{}}}"#;
        let responses = run_with(&format!("{}\n", req));
        let r = &responses[0];
        assert_eq!(r["id"], 3);
        let content = r["result"]["content"].as_array().unwrap();
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        // Body should be the JSON summary.
        let body: Value = serde_json::from_str(content[0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(body["models"], 1);
        assert_eq!(body["sources"], 1);
    }

    #[test]
    fn test_tools_call_unknown_tool_is_tool_error_not_jsonrpc_error() {
        let req = r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"bogus","arguments":{}}}"#;
        let responses = run_with(&format!("{}\n", req));
        let r = &responses[0];
        // Tool-level error: success at the JSON-RPC layer, isError=true in the result.
        assert!(r.get("error").is_none());
        assert_eq!(r["result"]["isError"], true);
    }

    #[test]
    fn test_unknown_method_returns_jsonrpc_error() {
        let req = r#"{"jsonrpc":"2.0","id":5,"method":"bogus"}"#;
        let responses = run_with(&format!("{}\n", req));
        assert_eq!(responses[0]["error"]["code"], -32601);
    }

    #[test]
    fn test_parse_error_returned_for_invalid_json() {
        let responses = run_with("not json\n");
        assert_eq!(responses[0]["error"]["code"], -32700);
    }

    #[test]
    fn test_multiple_requests_in_sequence() {
        let reqs = format!(
            "{}\n{}\n",
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#
        );
        let responses = run_with(&reqs);
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0]["id"], 1);
        assert_eq!(responses[1]["id"], 2);
    }
}
