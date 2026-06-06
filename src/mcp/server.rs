//! Minimal JSON-RPC 2.0 stdio server implementing the MCP handshake plus
//! `tools/list`, `tools/call`, `resources/list`, `resources/read`,
//! `prompts/list`, and `prompts/get`. Stays sync (no tokio) — one line in,
//! one line out — to match the rest of the project.

use std::io::{BufRead, Write};
use std::path::PathBuf;

use serde_json::{json, Value};

use crate::graph::types::LineageGraph;
use crate::mcp::{prompts, resources, tools};

/// Server identity sent back in the `initialize` response.
const SERVER_NAME: &str = "dbt-lineage";
const PROTOCOL_VERSION: &str = "2024-11-05";

/// Per-server state. Tools and resources both consult `graph`; `project_dir`
/// is used by tools that read source files (read_model_sql) and by resources
/// that expose `.sql` content.
pub struct McpContext {
    pub graph: LineageGraph,
    pub project_dir: PathBuf,
}

/// Run the MCP stdio server loop. Reads JSON-RPC requests line-by-line from
/// `input`, writes responses to `output`. Returns Ok when stdin closes cleanly.
pub fn run<R: BufRead, W: Write>(
    ctx: McpContext,
    mut input: R,
    output: &mut W,
) -> std::io::Result<()> {
    let mut line = String::new();
    loop {
        line.clear();
        let n = input.read_line(&mut line)?;
        if n == 0 {
            return Ok(());
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(req) => handle_message(&ctx, &req, output)?,
            Err(e) => {
                let resp = error_response(&Value::Null, -32700, &format!("parse error: {}", e));
                writeln_json(output, &resp)?;
            }
        }
    }
}

fn handle_message<W: Write>(ctx: &McpContext, req: &Value, output: &mut W) -> std::io::Result<()> {
    let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let is_notification = req.get("id").is_none();

    if is_notification {
        return Ok(());
    }

    let response = match method {
        "initialize" => Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {
                "tools": {},
                "resources": {},
                "prompts": {},
            },
            "serverInfo": {"name": SERVER_NAME, "version": env!("CARGO_PKG_VERSION")},
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(handle_tools_list()),
        "tools/call" => handle_tools_call(ctx, req.get("params")),
        "resources/list" => Ok(handle_resources_list(ctx)),
        "resources/read" => handle_resources_read(ctx, req.get("params")),
        "prompts/list" => Ok(handle_prompts_list()),
        "prompts/get" => handle_prompts_get(req.get("params")),
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

fn handle_tools_call(ctx: &McpContext, params: Option<&Value>) -> Result<Value, (i64, String)> {
    let params = params.ok_or_else(|| (-32602, "missing params".into()))?;
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| (-32602, "missing 'name' in params".into()))?;
    let empty = json!({});
    let arguments = params.get("arguments").unwrap_or(&empty);

    match tools::call_tool(ctx, name, arguments) {
        Ok(value) => {
            let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
            Ok(json!({
                "content": [{"type": "text", "text": text}],
                "isError": false,
            }))
        }
        Err(err) => Ok(json!({
            "content": [{"type": "text", "text": err.to_string()}],
            "isError": true,
        })),
    }
}

fn handle_resources_list(ctx: &McpContext) -> Value {
    let entries: Vec<Value> = resources::list(ctx)
        .into_iter()
        .map(|r| {
            json!({
                "uri": r.uri,
                "name": r.name,
                "description": r.description,
                "mimeType": r.mime_type,
            })
        })
        .collect();
    json!({"resources": entries})
}

fn handle_resources_read(ctx: &McpContext, params: Option<&Value>) -> Result<Value, (i64, String)> {
    let params = params.ok_or_else(|| (-32602, "missing params".into()))?;
    let uri = params
        .get("uri")
        .and_then(|v| v.as_str())
        .ok_or_else(|| (-32602, "missing 'uri' in params".into()))?;
    match resources::read(ctx, uri) {
        Ok(content) => Ok(json!({
            "contents": [{
                "uri": uri,
                "mimeType": content.mime_type,
                "text": content.text,
            }]
        })),
        Err(e) => Err((-32602, e.to_string())),
    }
}

fn handle_prompts_list() -> Value {
    let list: Vec<Value> = prompts::registry()
        .into_iter()
        .map(|p| {
            json!({
                "name": p.name,
                "description": p.description,
                "arguments": p.arguments,
            })
        })
        .collect();
    json!({"prompts": list})
}

fn handle_prompts_get(params: Option<&Value>) -> Result<Value, (i64, String)> {
    let params = params.ok_or_else(|| (-32602, "missing params".into()))?;
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| (-32602, "missing 'name' in params".into()))?;
    let empty = json!({});
    let arguments = params.get("arguments").unwrap_or(&empty);
    prompts::get(name, arguments).map_err(|e| (-32602, e.to_string()))
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

    fn make_ctx() -> McpContext {
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
            description: Some("Orders fact".into()),
            materialization: Some("table".into()),
            tags: vec!["finance".into()],
            columns: vec!["order_id".into()],
        });
        g.add_edge(
            s,
            m,
            EdgeData {
                edge_type: EdgeType::Source,
            },
        );
        McpContext {
            graph: g,
            project_dir: std::env::temp_dir(),
        }
    }

    fn run_with(input: &str) -> Vec<Value> {
        let mut out: Vec<u8> = Vec::new();
        run(make_ctx(), Cursor::new(input.as_bytes()), &mut out).unwrap();
        String::from_utf8(out)
            .unwrap()
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str::<Value>(l).expect("valid JSON"))
            .collect()
    }

    #[test]
    fn test_initialize_advertises_all_caps() {
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#;
        let r = &run_with(&format!("{}\n", req))[0];
        let caps = &r["result"]["capabilities"];
        assert!(caps.get("tools").is_some());
        assert!(caps.get("resources").is_some());
        assert!(caps.get("prompts").is_some());
    }

    #[test]
    fn test_resources_list_includes_model() {
        let req = r#"{"jsonrpc":"2.0","id":2,"method":"resources/list"}"#;
        let r = &run_with(&format!("{}\n", req))[0];
        let uris: Vec<&str> = r["result"]["resources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["uri"].as_str().unwrap())
            .collect();
        assert!(uris.iter().any(|u| u.contains("model/orders")));
    }

    #[test]
    fn test_prompts_list_contains_review_impact() {
        let req = r#"{"jsonrpc":"2.0","id":3,"method":"prompts/list"}"#;
        let r = &run_with(&format!("{}\n", req))[0];
        let names: Vec<&str> = r["result"]["prompts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"review-impact"));
    }

    #[test]
    fn test_tools_call_get_model_details() {
        let req = r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"get_model_details","arguments":{"model":"orders"}}}"#;
        let r = &run_with(&format!("{}\n", req))[0];
        assert_eq!(r["result"]["isError"], false);
        let text = r["result"]["content"][0]["text"].as_str().unwrap();
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["label"], "orders");
        assert_eq!(parsed["materialization"], "table");
    }
}
