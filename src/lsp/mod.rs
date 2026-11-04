//! `dbt-lineage lsp` — a stdio Language Server for dbt projects.
//!
//! Hover and go-to-definition on `ref()` / `source()`, a code lens with
//! upstream/downstream counts and impact on every model file, and `lint`
//! findings as diagnostics. Each file's project is the nearest ancestor with a
//! `dbt_project.yml`; its graph is built in SQL-parse mode (with the parse
//! cache) on first use and rebuilt on save.

pub mod analysis;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use lsp_server::{Connection, ErrorCode, Message, Notification, Request, Response};
use lsp_types::notification::Notification as _;
use lsp_types::request::Request as _;
use lsp_types::{
    CodeLens, CodeLensOptions, CodeLensParams, Command, Diagnostic, DiagnosticSeverity,
    DidChangeTextDocumentParams, DidCloseTextDocumentParams, DidOpenTextDocumentParams,
    DidSaveTextDocumentParams, GotoDefinitionParams, GotoDefinitionResponse, Hover, HoverContents,
    HoverParams, HoverProviderCapability, Location, MarkupContent, MarkupKind, OneOf, Position,
    PublishDiagnosticsParams, Range, ServerCapabilities, TextDocumentSyncCapability,
    TextDocumentSyncKind, TextDocumentSyncOptions, TextDocumentSyncSaveOptions, Url,
};

use crate::graph::lint::LintSeverity;
use analysis::{find_project_root, ref_at, Analysis};

/// Command the code lens triggers; editors that don't register it ignore it.
pub const SHOW_LINEAGE_COMMAND: &str = "dbt-lineage.showLineage";

/// Serve over stdin/stdout until the client shuts down.
pub fn run_stdio() -> Result<()> {
    let (connection, io_threads) = Connection::stdio();
    run(connection)?;
    io_threads.join()?;
    Ok(())
}

/// Server capabilities advertised at `initialize`.
pub fn capabilities() -> ServerCapabilities {
    ServerCapabilities {
        text_document_sync: Some(TextDocumentSyncCapability::Options(
            TextDocumentSyncOptions {
                open_close: Some(true),
                change: Some(TextDocumentSyncKind::FULL),
                save: Some(TextDocumentSyncSaveOptions::Supported(true)),
                ..Default::default()
            },
        )),
        hover_provider: Some(HoverProviderCapability::Simple(true)),
        definition_provider: Some(OneOf::Left(true)),
        code_lens_provider: Some(CodeLensOptions {
            resolve_provider: Some(false),
        }),
        ..Default::default()
    }
}

/// Run the initialize handshake and the message loop on `connection`.
pub fn run(connection: Connection) -> Result<()> {
    connection.initialize(serde_json::to_value(capabilities())?)?;
    let mut server = Server::default();
    for msg in &connection.receiver {
        match msg {
            Message::Request(req) => {
                if connection.handle_shutdown(&req)? {
                    return Ok(());
                }
                connection
                    .sender
                    .send(Message::Response(server.request(req)))?;
            }
            Message::Notification(note) => {
                for out in server.notification(note) {
                    connection.sender.send(out)?;
                }
            }
            Message::Response(_) => {}
        }
    }
    Ok(())
}

/// Open buffers plus one analysis per dbt project.
#[derive(Default)]
pub struct Server {
    docs: HashMap<Url, String>,
    projects: HashMap<PathBuf, Option<Analysis>>,
}

impl Server {
    fn request(&mut self, req: Request) -> Response {
        let id = req.id.clone();
        let result = match req.method.as_str() {
            lsp_types::request::HoverRequest::METHOD => serde_json::from_value(req.params)
                .map(|p| serde_json::to_value(self.hover(p)).unwrap()),
            lsp_types::request::GotoDefinition::METHOD => serde_json::from_value(req.params)
                .map(|p| serde_json::to_value(self.definition(p)).unwrap()),
            lsp_types::request::CodeLensRequest::METHOD => serde_json::from_value(req.params)
                .map(|p| serde_json::to_value(self.code_lens(p)).unwrap()),
            _ => {
                return Response::new_err(
                    id,
                    ErrorCode::MethodNotFound as i32,
                    format!("unsupported request {}", req.method),
                )
            }
        };
        match result {
            Ok(value) => Response::new_ok(id, value),
            Err(e) => Response::new_err(id, ErrorCode::InvalidParams as i32, e.to_string()),
        }
    }

    fn notification(&mut self, note: Notification) -> Vec<Message> {
        match note.method.as_str() {
            lsp_types::notification::DidOpenTextDocument::METHOD => {
                let Ok(p) = serde_json::from_value::<DidOpenTextDocumentParams>(note.params) else {
                    return vec![];
                };
                let uri = p.text_document.uri;
                self.docs.insert(uri.clone(), p.text_document.text);
                self.diagnostics(&uri).into_iter().collect()
            }
            lsp_types::notification::DidChangeTextDocument::METHOD => {
                if let Ok(p) = serde_json::from_value::<DidChangeTextDocumentParams>(note.params) {
                    // Full sync: the last change carries the whole text.
                    if let Some(change) = p.content_changes.into_iter().last() {
                        self.docs.insert(p.text_document.uri, change.text);
                    }
                }
                vec![]
            }
            lsp_types::notification::DidSaveTextDocument::METHOD => {
                let Ok(p) = serde_json::from_value::<DidSaveTextDocumentParams>(note.params) else {
                    return vec![];
                };
                // Rebuild the project, then refresh every open file in it.
                let Some(root) = path_of(&p.text_document.uri).and_then(|f| find_project_root(&f))
                else {
                    return vec![];
                };
                self.projects.remove(&root);
                let open: Vec<Url> = self
                    .docs
                    .keys()
                    .filter(|u| path_of(u).is_some_and(|f| f.starts_with(&root)))
                    .cloned()
                    .collect();
                open.iter().filter_map(|u| self.diagnostics(u)).collect()
            }
            lsp_types::notification::DidCloseTextDocument::METHOD => {
                let Ok(p) = serde_json::from_value::<DidCloseTextDocumentParams>(note.params)
                else {
                    return vec![];
                };
                self.docs.remove(&p.text_document.uri);
                vec![publish(p.text_document.uri, vec![])]
            }
            _ => vec![],
        }
    }

    /// Analysis for the project containing `file`, built on first use.
    fn analysis(&mut self, file: &Path) -> Option<&Analysis> {
        let root = find_project_root(file)?;
        self.projects
            .entry(root.clone())
            .or_insert_with(|| match Analysis::load(&root) {
                Ok(a) => Some(a),
                Err(e) => {
                    eprintln!("dbt-lineage lsp: cannot load {}: {:#}", root.display(), e);
                    None
                }
            })
            .as_ref()
    }

    fn hover(&mut self, p: HoverParams) -> Option<Hover> {
        let pos = p.text_document_position_params;
        let file = path_of(&pos.text_document.uri)?;
        let text = self.docs.get(&pos.text_document.uri)?.clone();
        let call = ref_at(&text, pos.position.line, pos.position.character)?;
        let analysis = self.analysis(&file)?;
        let idx = analysis.resolve(&call.kind)?;
        Some(Hover {
            contents: HoverContents::Markup(MarkupContent {
                kind: MarkupKind::Markdown,
                value: analysis.hover_markdown(idx),
            }),
            range: Some(range(call.line, call.start, call.end)),
        })
    }

    fn definition(&mut self, p: GotoDefinitionParams) -> Option<GotoDefinitionResponse> {
        let pos = p.text_document_position_params;
        let file = path_of(&pos.text_document.uri)?;
        let text = self.docs.get(&pos.text_document.uri)?.clone();
        let call = ref_at(&text, pos.position.line, pos.position.character)?;
        let (target, line) = self.analysis(&file)?.definition(&call.kind)?;
        Some(GotoDefinitionResponse::Scalar(Location {
            uri: Url::from_file_path(target).ok()?,
            range: range(line, 0, 0),
        }))
    }

    fn code_lens(&mut self, p: CodeLensParams) -> Option<Vec<CodeLens>> {
        let file = path_of(&p.text_document.uri)?;
        let analysis = self.analysis(&file)?;
        let idx = analysis.node_for_file(&file)?;
        Some(vec![CodeLens {
            range: range(0, 0, 0),
            command: Some(Command {
                title: analysis.lens_title(idx),
                command: SHOW_LINEAGE_COMMAND.into(),
                arguments: Some(vec![serde_json::json!(analysis.graph[idx].label)]),
            }),
            data: None,
        }])
    }

    fn diagnostics(&mut self, uri: &Url) -> Option<Message> {
        let file = path_of(uri)?;
        let text = self.docs.get(uri)?.clone();
        let findings = self.analysis(&file)?.findings_for(&file, &text);
        let diags = findings
            .into_iter()
            .map(|f| Diagnostic {
                range: range(f.line, f.start, f.end),
                severity: Some(match f.severity {
                    LintSeverity::Error => DiagnosticSeverity::ERROR,
                    LintSeverity::Warning => DiagnosticSeverity::WARNING,
                    LintSeverity::Info => DiagnosticSeverity::INFORMATION,
                }),
                code: Some(lsp_types::NumberOrString::String(f.rule.into())),
                source: Some("dbt-lineage".into()),
                message: f.message,
                ..Default::default()
            })
            .collect();
        Some(publish(uri.clone(), diags))
    }
}

fn path_of(uri: &Url) -> Option<PathBuf> {
    uri.to_file_path().ok()
}

fn range(line: u32, start: u32, end: u32) -> Range {
    Range::new(Position::new(line, start), Position::new(line, end))
}

fn publish(uri: Url, diagnostics: Vec<Diagnostic>) -> Message {
    Message::Notification(Notification::new(
        lsp_types::notification::PublishDiagnostics::METHOD.into(),
        PublishDiagnosticsParams {
            uri,
            diagnostics,
            version: None,
        },
    ))
}
