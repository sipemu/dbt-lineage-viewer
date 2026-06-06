//! Minimal MCP (Model Context Protocol) server.
//!
//! Speaks JSON-RPC 2.0 over stdio, line-oriented (one JSON object per line). The
//! server loads a manifest.json at startup, builds the lineage graph once, and
//! serves a small set of tools that AI agents can call directly instead of
//! shelling out to the CLI and parsing text. See GH issue #3.

pub mod server;
pub mod tools;
