//! Drive `dbt-lineage lsp` over in-memory channels:
//! initialize → didOpen → hover / definition / codeLens → shutdown.
#![cfg(feature = "lsp")]

use std::path::Path;
use std::time::Duration;

use lsp_server::{Connection, Message, Notification, Request, RequestId, Response};
use serde_json::{json, Value};

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            if entry.file_name() != ".dbt-lineage" && entry.file_name() != "target" {
                copy_dir(&entry.path(), &target);
            }
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

struct Client {
    conn: Connection,
    next_id: i32,
}

impl Client {
    fn request(&mut self, method: &str, params: Value) -> Response {
        self.next_id += 1;
        let id = RequestId::from(self.next_id);
        self.conn
            .sender
            .send(Message::Request(Request::new(
                id.clone(),
                method.into(),
                params,
            )))
            .unwrap();
        loop {
            match self.recv() {
                Message::Response(r) if r.id == id => return r,
                _ => continue, // diagnostics etc.
            }
        }
    }

    fn notify(&self, method: &str, params: Value) {
        self.conn
            .sender
            .send(Message::Notification(Notification::new(
                method.into(),
                params,
            )))
            .unwrap();
    }

    fn recv(&self) -> Message {
        self.conn
            .receiver
            .recv_timeout(Duration::from_secs(20))
            .expect("server did not answer")
    }

    /// Next publishDiagnostics notification for `uri`.
    fn diagnostics_for(&self, uri: &str) -> Vec<Value> {
        loop {
            if let Message::Notification(n) = self.recv() {
                if n.method == "textDocument/publishDiagnostics" && n.params["uri"] == uri {
                    return n.params["diagnostics"].as_array().unwrap().clone();
                }
            }
        }
    }
}

#[test]
fn lsp_session_over_memory_channels() {
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path().join("project");
    copy_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/simple_project"),
        &project,
    );
    // An undefined ref so diagnostics have something to report.
    let model = project.join("models/marts/orders.sql");
    let text = std::fs::read_to_string(&model).unwrap().replace(
        "{{ ref('stg_payments') }}",
        "{{ ref('stg_payments') }} join {{ ref('ghost') }} g on true",
    );
    std::fs::write(&model, &text).unwrap();
    let uri = url::Url::from_file_path(&model).unwrap().to_string();

    let (server_conn, client_conn) = Connection::memory();
    let server = std::thread::spawn(move || dbt_lineage::lsp::run(server_conn));
    let mut client = Client {
        conn: client_conn,
        next_id: 0,
    };

    let init = client.request(
        "initialize",
        json!({"capabilities": {}, "rootUri": url::Url::from_directory_path(&project).unwrap()}),
    );
    let caps = &init.result.unwrap()["capabilities"];
    assert_eq!(caps["hoverProvider"], true);
    assert_eq!(caps["definitionProvider"], true);
    client.notify("initialized", json!({}));

    client.notify(
        "textDocument/didOpen",
        json!({"textDocument": {"uri": uri, "languageId": "sql", "version": 1, "text": text}}),
    );
    let diags = client.diagnostics_for(&uri);
    let ghost = diags
        .iter()
        .find(|d| d["severity"] == 1)
        .unwrap_or_else(|| panic!("no error diagnostic: {diags:?}"));
    assert!(
        ghost["message"].as_str().unwrap().contains("ghost"),
        "{ghost}"
    );
    assert_eq!(ghost["source"], "dbt-lineage");

    // Hover on ref('stg_orders').
    let (line, col) = text
        .lines()
        .enumerate()
        .find_map(|(i, l)| l.find("ref('stg_orders')").map(|c| (i, c + 5)))
        .unwrap();
    let hover = client.request(
        "textDocument/hover",
        json!({"textDocument": {"uri": uri}, "position": {"line": line, "character": col}}),
    );
    let md = hover.result.unwrap()["contents"]["value"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(md.contains("`stg_orders`"), "{md}");
    assert!(md.contains("upstream"), "{md}");

    let def = client.request(
        "textDocument/definition",
        json!({"textDocument": {"uri": uri}, "position": {"line": line, "character": col}}),
    );
    let target = def.result.unwrap()["uri"].as_str().unwrap().to_string();
    assert!(
        target.ends_with("models/staging/stg_orders.sql"),
        "{target}"
    );

    let lens = client.request(
        "textDocument/codeLens",
        json!({"textDocument": {"uri": uri}}),
    );
    let lens = lens.result.unwrap();
    let title = lens[0]["command"]["title"].as_str().unwrap();
    assert!(
        title.contains("upstream ·") && title.contains("impact:"),
        "{title}"
    );
    assert_eq!(lens[0]["command"]["arguments"][0], "orders");

    // Hover outside a ref → null.
    let none = client.request(
        "textDocument/hover",
        json!({"textDocument": {"uri": uri}, "position": {"line": 0, "character": 0}}),
    );
    assert_eq!(none.result.unwrap(), Value::Null);

    // Fix the file and save: diagnostics refresh without the error.
    let fixed = text.replace(" join {{ ref('ghost') }} g on true", "");
    std::fs::write(&model, &fixed).unwrap();
    client.notify(
        "textDocument/didChange",
        json!({"textDocument": {"uri": uri, "version": 2}, "contentChanges": [{"text": fixed}]}),
    );
    client.notify(
        "textDocument/didSave",
        json!({"textDocument": {"uri": uri}}),
    );
    let diags = client.diagnostics_for(&uri);
    assert!(diags.iter().all(|d| d["severity"] != 1), "{diags:?}");

    let shutdown = client.request("shutdown", Value::Null);
    assert!(shutdown.error.is_none());
    client.notify("exit", Value::Null);
    server.join().unwrap().unwrap();
}
