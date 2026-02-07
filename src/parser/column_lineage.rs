use std::collections::HashMap;

use regex::Regex;
use serde::Serialize;
use std::sync::LazyLock;

use crate::graph::types::LineageGraph;

/// Confidence level for a column-level edge
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnConfidence {
    /// Direct column reference (e.g., `SELECT order_id FROM ...`)
    Direct,
    /// Aliased column (e.g., `SELECT o.order_id AS id`)
    Aliased,
    /// Derived from expression (e.g., `SELECT SUM(amount) AS total`)
    Derived,
    /// Star expansion (e.g., `SELECT *`)
    Star,
}

impl ColumnConfidence {
    pub fn label(&self) -> &'static str {
        match self {
            ColumnConfidence::Direct => "Direct",
            ColumnConfidence::Aliased => "Aliased",
            ColumnConfidence::Derived => "Derived",
            ColumnConfidence::Star => "Star",
        }
    }
}

/// A column-level lineage edge
#[derive(Debug, Clone, Serialize)]
pub struct ColumnEdge {
    pub source_node: String,
    pub source_column: String,
    pub target_node: String,
    pub target_column: String,
    pub confidence: ColumnConfidence,
}

/// All column-level lineage information
#[derive(Debug, Clone, Default, Serialize)]
pub struct ColumnLineage {
    pub edges: Vec<ColumnEdge>,
}

impl ColumnLineage {
    /// Get all column edges for a target node
    pub fn edges_for_target(&self, target_node: &str) -> Vec<&ColumnEdge> {
        self.edges
            .iter()
            .filter(|e| e.target_node == target_node)
            .collect()
    }
}

/// A table reference extracted from FROM/JOIN clauses
#[derive(Debug, Clone)]
pub struct TableRef {
    /// The alias used in the SQL (e.g., "o" in "FROM orders o")
    pub alias: Option<String>,
    /// The resolved node unique_id (e.g., "model.orders")
    pub node_id: String,
}

/// A single SELECT item with source tracking
#[derive(Debug, Clone)]
pub struct SelectItem {
    /// Output column name
    pub column_name: String,
    /// Source alias (table reference) if identifiable
    pub source_alias: Option<String>,
    /// Source column if identifiable
    pub source_column: Option<String>,
    /// Whether this is a `SELECT *`
    pub is_star: bool,
    /// Whether this is a derived expression
    pub is_derived: bool,
}

/// Regex for FROM/JOIN table references with optional alias
static TABLE_REF_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(?:FROM|JOIN)\s+\{\{\s*(?:ref\(\s*'([^']+)'\s*\)|source\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\))\s*\}\}(?:\s+(?:AS\s+)?(\w+))?"
    )
    .unwrap()
});

/// Regex for simple column references: `alias.column` or `column`
#[allow(dead_code)]
static COLUMN_REF_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^(\w+)\.(\w+)$").unwrap());

/// Extract table references from SQL (FROM/JOIN clauses with ref()/source())
pub fn extract_table_refs(sql: &str) -> Vec<TableRef> {
    let mut refs = Vec::new();

    for cap in TABLE_REF_RE.captures_iter(sql) {
        let alias = cap.get(4).map(|m| m.as_str().to_string());

        if let Some(ref_name) = cap.get(1) {
            refs.push(TableRef {
                alias,
                node_id: format!("model.{}", ref_name.as_str()),
            });
        } else if let (Some(source_name), Some(table_name)) = (cap.get(2), cap.get(3)) {
            refs.push(TableRef {
                alias,
                node_id: format!("source.{}.{}", source_name.as_str(), table_name.as_str()),
            });
        }
    }

    refs
}

/// Resolve column lineage for an entire graph
pub fn resolve_column_lineage(graph: &LineageGraph) -> ColumnLineage {
    let mut edges = Vec::new();

    // Build a map of unique_id -> columns for source resolution
    let column_map: HashMap<String, Vec<String>> = graph
        .node_indices()
        .map(|idx| {
            let node = &graph[idx];
            (node.unique_id.clone(), node.columns.clone())
        })
        .collect();

    // For each model node with a file_path, try to resolve column lineage
    for idx in graph.node_indices() {
        let node = &graph[idx];
        let Some(file_path) = &node.file_path else {
            continue;
        };

        // Read the SQL file
        let sql = match std::fs::read_to_string(file_path) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let table_refs = extract_table_refs(&sql);
        let select_items = extract_select_items(&sql);

        // Build alias -> node_id map
        let alias_map: HashMap<String, String> = table_refs
            .iter()
            .filter_map(|tr| tr.alias.as_ref().map(|a| (a.clone(), tr.node_id.clone())))
            .collect();

        // If there's exactly one table ref with no alias, it's the default source
        let default_source = if table_refs.len() == 1 {
            Some(table_refs[0].node_id.clone())
        } else {
            None
        };

        for item in &select_items {
            if item.is_star {
                // Star: create star edges for all upstream sources
                for tr in &table_refs {
                    if let Some(upstream_cols) = column_map.get(&tr.node_id) {
                        for col in upstream_cols {
                            edges.push(ColumnEdge {
                                source_node: tr.node_id.clone(),
                                source_column: col.clone(),
                                target_node: node.unique_id.clone(),
                                target_column: col.clone(),
                                confidence: ColumnConfidence::Star,
                            });
                        }
                    }
                }
                continue;
            }

            if item.is_derived {
                // Derived: we know the output column but not the source
                edges.push(ColumnEdge {
                    source_node: default_source.clone().unwrap_or_default(),
                    source_column: String::new(),
                    target_node: node.unique_id.clone(),
                    target_column: item.column_name.clone(),
                    confidence: ColumnConfidence::Derived,
                });
                continue;
            }

            // Try to resolve the source
            let resolved_source = if let Some(alias) = &item.source_alias {
                alias_map
                    .get(alias)
                    .cloned()
                    .or_else(|| default_source.clone())
            } else {
                default_source.clone()
            };

            let source_col = item.source_column.as_ref().unwrap_or(&item.column_name);

            let confidence = if item.source_alias.is_some()
                && item.source_column.as_ref() != Some(&item.column_name)
            {
                ColumnConfidence::Aliased
            } else {
                ColumnConfidence::Direct
            };

            if let Some(source_node) = resolved_source {
                edges.push(ColumnEdge {
                    source_node,
                    source_column: source_col.clone(),
                    target_node: node.unique_id.clone(),
                    target_column: item.column_name.clone(),
                    confidence,
                });
            }
        }
    }

    ColumnLineage { edges }
}

/// Extract SELECT items with source alias tracking from SQL
pub fn extract_select_items(sql: &str) -> Vec<SelectItem> {
    // Reuse the column extraction logic but with richer output
    let columns = crate::parser::columns::extract_select_columns(sql);
    let mut items = Vec::new();

    // Strip Jinja and find the SELECT body for detailed parsing
    let cleaned = strip_jinja(sql);

    for col in &columns {
        if col == "*" {
            items.push(SelectItem {
                column_name: "*".to_string(),
                source_alias: None,
                source_column: None,
                is_star: true,
                is_derived: false,
            });
            continue;
        }

        if col == "__jinja__" {
            items.push(SelectItem {
                column_name: col.clone(),
                source_alias: None,
                source_column: None,
                is_star: false,
                is_derived: true,
            });
            continue;
        }

        // Try to find this column's source in the cleaned SQL
        let (source_alias, source_column, is_derived) = find_column_source(&cleaned, col);

        items.push(SelectItem {
            column_name: col.clone(),
            source_alias,
            source_column,
            is_star: false,
            is_derived,
        });
    }

    items
}

/// Strip Jinja tags and comments from SQL
fn strip_jinja(sql: &str) -> String {
    let re_comment = Regex::new(r"\{#[\s\S]*?#\}").unwrap();
    let re_tag = Regex::new(r"\{\{-?[\s\S]*?-?\}\}|\{%-?[\s\S]*?-?%\}").unwrap();
    let cleaned = re_comment.replace_all(sql, "");
    re_tag.replace_all(&cleaned, "__jinja__").to_string()
}

/// Try to find the source alias and column for an output column name
fn find_column_source(
    cleaned_sql: &str,
    output_col: &str,
) -> (Option<String>, Option<String>, bool) {
    // Look for patterns like `alias.column AS output_col` or `alias.output_col`
    let pattern = format!(
        r"(?i)(\w+)\.(\w+)\s+(?:AS\s+)?{}(?:\s|,|$)",
        regex::escape(output_col)
    );
    if let Ok(re) = Regex::new(&pattern) {
        if let Some(cap) = re.captures(cleaned_sql) {
            return (Some(cap[1].to_string()), Some(cap[2].to_string()), false);
        }
    }

    // Check for direct `alias.column` pattern where column == output_col
    let direct_pattern = format!(r"(?i)(\w+)\.{}\b", regex::escape(output_col));
    if let Ok(re) = Regex::new(&direct_pattern) {
        if let Some(cap) = re.captures(cleaned_sql) {
            return (
                Some(cap[1].to_string()),
                Some(output_col.to_string()),
                false,
            );
        }
    }

    // Check if this looks like a function call (derived)
    let func_pattern = format!(
        r"(?i)\w+\s*\([^)]*\)\s+(?:AS\s+)?{}",
        regex::escape(output_col)
    );
    if let Ok(re) = Regex::new(&func_pattern) {
        if re.is_match(cleaned_sql) {
            return (None, None, true);
        }
    }

    // Simple column reference without alias
    (None, Some(output_col.to_string()), false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_confidence_labels() {
        assert_eq!(ColumnConfidence::Direct.label(), "Direct");
        assert_eq!(ColumnConfidence::Aliased.label(), "Aliased");
        assert_eq!(ColumnConfidence::Derived.label(), "Derived");
        assert_eq!(ColumnConfidence::Star.label(), "Star");
    }

    #[test]
    fn test_extract_table_refs_ref() {
        let sql = "SELECT * FROM {{ ref('stg_orders') }} o";
        let refs = extract_table_refs(sql);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].node_id, "model.stg_orders");
        assert_eq!(refs[0].alias.as_deref(), Some("o"));
    }

    #[test]
    fn test_extract_table_refs_source() {
        let sql = "SELECT * FROM {{ source('raw', 'orders') }}";
        let refs = extract_table_refs(sql);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].node_id, "source.raw.orders");
        assert!(refs[0].alias.is_none());
    }

    #[test]
    fn test_extract_table_refs_multiple() {
        let sql = r#"
            SELECT *
            FROM {{ ref('stg_orders') }} o
            JOIN {{ ref('stg_payments') }} p ON o.id = p.order_id
        "#;
        let refs = extract_table_refs(sql);
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].node_id, "model.stg_orders");
        assert_eq!(refs[0].alias.as_deref(), Some("o"));
        assert_eq!(refs[1].node_id, "model.stg_payments");
        assert_eq!(refs[1].alias.as_deref(), Some("p"));
    }

    #[test]
    fn test_extract_table_refs_no_alias() {
        let sql = "SELECT * FROM {{ ref('orders') }}";
        let refs = extract_table_refs(sql);
        assert_eq!(refs.len(), 1);
        assert!(refs[0].alias.is_none());
    }

    #[test]
    fn test_extract_select_items_simple() {
        let sql = "SELECT order_id, customer_id FROM {{ ref('stg_orders') }}";
        let items = extract_select_items(sql);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].column_name, "order_id");
        assert!(!items[0].is_star);
        assert!(!items[0].is_derived);
    }

    #[test]
    fn test_extract_select_items_star() {
        let sql = "SELECT * FROM {{ ref('stg_orders') }}";
        let items = extract_select_items(sql);
        assert_eq!(items.len(), 1);
        assert!(items[0].is_star);
    }

    #[test]
    fn test_extract_select_items_with_alias() {
        let sql = "SELECT o.order_id, o.customer_id FROM {{ ref('stg_orders') }} o";
        let items = extract_select_items(sql);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].column_name, "order_id");
    }

    #[test]
    fn test_column_lineage_default() {
        let lineage = ColumnLineage::default();
        assert!(lineage.edges.is_empty());
    }

    #[test]
    fn test_column_lineage_edges_for_target() {
        let lineage = ColumnLineage {
            edges: vec![
                ColumnEdge {
                    source_node: "model.a".to_string(),
                    source_column: "col1".to_string(),
                    target_node: "model.b".to_string(),
                    target_column: "col1".to_string(),
                    confidence: ColumnConfidence::Direct,
                },
                ColumnEdge {
                    source_node: "model.a".to_string(),
                    source_column: "col2".to_string(),
                    target_node: "model.c".to_string(),
                    target_column: "col2".to_string(),
                    confidence: ColumnConfidence::Direct,
                },
            ],
        };

        let edges = lineage.edges_for_target("model.b");
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].target_column, "col1");

        let edges = lineage.edges_for_target("model.c");
        assert_eq!(edges.len(), 1);
    }

    #[test]
    fn test_strip_jinja() {
        let sql = "{{ config(materialized='table') }} SELECT * FROM {{ ref('orders') }}";
        let cleaned = strip_jinja(sql);
        assert!(!cleaned.contains("{{"));
        assert!(cleaned.contains("__jinja__"));
    }

    #[test]
    fn test_resolve_column_lineage_direct_columns() {
        let tmp = tempfile::tempdir().unwrap();
        let sql_path = tmp.path().join("stg_customers.sql");
        std::fs::write(
            &sql_path,
            "SELECT order_id, customer_id FROM {{ ref('stg_orders') }}",
        )
        .unwrap();

        let mut graph = LineageGraph::new();
        graph.add_node(crate::graph::types::NodeData {
            unique_id: "model.stg_orders".into(),
            label: "stg_orders".into(),
            node_type: crate::graph::types::NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec!["order_id".into(), "customer_id".into(), "amount".into()],
        });
        graph.add_node(crate::graph::types::NodeData {
            unique_id: "model.stg_customers".into(),
            label: "stg_customers".into(),
            node_type: crate::graph::types::NodeType::Model,
            file_path: Some(sql_path.clone()),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });

        let lineage = resolve_column_lineage(&graph);
        let edges: Vec<_> = lineage
            .edges
            .iter()
            .filter(|e| e.target_node == "model.stg_customers")
            .collect();
        assert_eq!(edges.len(), 2);
        assert!(edges
            .iter()
            .all(|e| e.confidence == ColumnConfidence::Direct));
        assert!(edges.iter().any(|e| e.target_column == "order_id"));
        assert!(edges.iter().any(|e| e.target_column == "customer_id"));
    }

    #[test]
    fn test_resolve_column_lineage_star_expansion() {
        let tmp = tempfile::tempdir().unwrap();
        let sql_path = tmp.path().join("mart.sql");
        std::fs::write(&sql_path, "SELECT * FROM {{ ref('stg_orders') }}").unwrap();

        let mut graph = LineageGraph::new();
        graph.add_node(crate::graph::types::NodeData {
            unique_id: "model.stg_orders".into(),
            label: "stg_orders".into(),
            node_type: crate::graph::types::NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec!["order_id".into(), "status".into()],
        });
        graph.add_node(crate::graph::types::NodeData {
            unique_id: "model.mart".into(),
            label: "mart".into(),
            node_type: crate::graph::types::NodeType::Model,
            file_path: Some(sql_path),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });

        let lineage = resolve_column_lineage(&graph);
        let star_edges: Vec<_> = lineage
            .edges
            .iter()
            .filter(|e| e.confidence == ColumnConfidence::Star)
            .collect();
        assert_eq!(star_edges.len(), 2);
        assert!(star_edges.iter().any(|e| e.target_column == "order_id"));
        assert!(star_edges.iter().any(|e| e.target_column == "status"));
    }

    #[test]
    fn test_resolve_column_lineage_aliased_columns() {
        let tmp = tempfile::tempdir().unwrap();
        let sql_path = tmp.path().join("model_a.sql");
        std::fs::write(
            &sql_path,
            "SELECT o.order_id AS id FROM {{ ref('stg_orders') }} o",
        )
        .unwrap();

        let mut graph = LineageGraph::new();
        graph.add_node(crate::graph::types::NodeData {
            unique_id: "model.stg_orders".into(),
            label: "stg_orders".into(),
            node_type: crate::graph::types::NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec!["order_id".into()],
        });
        graph.add_node(crate::graph::types::NodeData {
            unique_id: "model.model_a".into(),
            label: "model_a".into(),
            node_type: crate::graph::types::NodeType::Model,
            file_path: Some(sql_path),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });

        let lineage = resolve_column_lineage(&graph);
        let aliased: Vec<_> = lineage
            .edges
            .iter()
            .filter(|e| e.target_node == "model.model_a")
            .collect();
        assert_eq!(aliased.len(), 1);
        assert_eq!(aliased[0].confidence, ColumnConfidence::Aliased);
        assert_eq!(aliased[0].target_column, "id");
        assert_eq!(aliased[0].source_column, "order_id");
    }

    #[test]
    fn test_resolve_column_lineage_derived_columns() {
        let tmp = tempfile::tempdir().unwrap();
        let sql_path = tmp.path().join("model_b.sql");
        std::fs::write(
            &sql_path,
            "SELECT SUM(amount) AS total FROM {{ ref('stg_orders') }}",
        )
        .unwrap();

        let mut graph = LineageGraph::new();
        graph.add_node(crate::graph::types::NodeData {
            unique_id: "model.stg_orders".into(),
            label: "stg_orders".into(),
            node_type: crate::graph::types::NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec!["amount".into()],
        });
        graph.add_node(crate::graph::types::NodeData {
            unique_id: "model.model_b".into(),
            label: "model_b".into(),
            node_type: crate::graph::types::NodeType::Model,
            file_path: Some(sql_path),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });

        let lineage = resolve_column_lineage(&graph);
        let derived: Vec<_> = lineage
            .edges
            .iter()
            .filter(|e| e.target_node == "model.model_b")
            .collect();
        assert_eq!(derived.len(), 1);
        assert_eq!(derived[0].confidence, ColumnConfidence::Derived);
        assert_eq!(derived[0].target_column, "total");
    }

    #[test]
    fn test_resolve_column_lineage_missing_file() {
        let mut graph = LineageGraph::new();
        graph.add_node(crate::graph::types::NodeData {
            unique_id: "model.missing".into(),
            label: "missing".into(),
            node_type: crate::graph::types::NodeType::Model,
            file_path: Some("/nonexistent/path/model.sql".into()),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });

        let lineage = resolve_column_lineage(&graph);
        assert!(lineage.edges.is_empty());
    }

    #[test]
    fn test_find_column_source_alias_as() {
        let sql = "SELECT o.order_id AS oid, o.status FROM orders o";
        let (alias, col, derived) = find_column_source(sql, "oid");
        assert_eq!(alias.as_deref(), Some("o"));
        assert_eq!(col.as_deref(), Some("order_id"));
        assert!(!derived);
    }

    #[test]
    fn test_find_column_source_function_call() {
        let sql = "SELECT COUNT(*) AS cnt FROM orders";
        let (alias, col, derived) = find_column_source(sql, "cnt");
        assert!(alias.is_none());
        assert!(col.is_none());
        assert!(derived);
    }

    #[test]
    fn test_find_column_source_no_match() {
        let sql = "SELECT something_else FROM orders";
        let (alias, col, derived) = find_column_source(sql, "order_id");
        assert!(alias.is_none());
        assert_eq!(col.as_deref(), Some("order_id"));
        assert!(!derived);
    }

    #[test]
    fn test_extract_select_items_derived() {
        let sql = "SELECT {{ dbt_utils.star(from=ref('x')) }}, order_id FROM {{ ref('x') }}";
        let items = extract_select_items(sql);
        assert!(items
            .iter()
            .any(|i| i.column_name == "__jinja__" && i.is_derived));
        assert!(items
            .iter()
            .any(|i| i.column_name == "order_id" && !i.is_derived));
    }

    #[test]
    fn test_resolve_column_lineage_multiple_table_refs() {
        // Covers line 159: default_source = None when multiple table refs
        let tmp = tempfile::tempdir().unwrap();
        let sql_path = tmp.path().join("joined.sql");
        std::fs::write(
            &sql_path,
            "SELECT o.order_id, c.name FROM {{ ref('orders') }} o JOIN {{ ref('customers') }} c ON o.customer_id = c.id",
        )
        .unwrap();

        let mut graph = LineageGraph::new();
        graph.add_node(crate::graph::types::NodeData {
            unique_id: "model.orders".into(),
            label: "orders".into(),
            node_type: crate::graph::types::NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec!["order_id".into(), "customer_id".into()],
        });
        graph.add_node(crate::graph::types::NodeData {
            unique_id: "model.customers".into(),
            label: "customers".into(),
            node_type: crate::graph::types::NodeType::Model,
            file_path: None,
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec!["id".into(), "name".into()],
        });
        graph.add_node(crate::graph::types::NodeData {
            unique_id: "model.joined".into(),
            label: "joined".into(),
            node_type: crate::graph::types::NodeType::Model,
            file_path: Some(sql_path),
            description: None,
            materialization: None,
            tags: vec![],
            columns: vec![],
        });

        let lineage = resolve_column_lineage(&graph);
        let joined_edges: Vec<_> = lineage
            .edges
            .iter()
            .filter(|e| e.target_node == "model.joined")
            .collect();
        // Should have edges for order_id and name
        assert!(!joined_edges.is_empty());
    }
}
