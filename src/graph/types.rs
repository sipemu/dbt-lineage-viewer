use petgraph::stable_graph::StableDiGraph;
use std::path::PathBuf;

/// The lineage DAG type
pub type LineageGraph = StableDiGraph<NodeData, EdgeData>;

/// Types of nodes in the dbt lineage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeType {
    Model,
    Source,
    Seed,
    Snapshot,
    Test,
    Exposure,
    /// Unresolved reference (phantom node)
    Phantom,
}

impl NodeType {
    pub fn prefix(&self) -> &'static str {
        match self {
            NodeType::Model => "",
            NodeType::Source => "src:",
            NodeType::Seed => "seed:",
            NodeType::Snapshot => "snap:",
            NodeType::Test => "test:",
            NodeType::Exposure => "exp:",
            NodeType::Phantom => "?:",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            NodeType::Model => "model",
            NodeType::Source => "source",
            NodeType::Seed => "seed",
            NodeType::Snapshot => "snapshot",
            NodeType::Test => "test",
            NodeType::Exposure => "exposure",
            NodeType::Phantom => "phantom",
        }
    }
}

/// Data associated with each node
#[derive(Debug, Clone)]
pub struct NodeData {
    /// Unique identifier (e.g., "model.stg_orders" or "source.raw.orders")
    pub unique_id: String,
    /// Display label (e.g., "stg_orders")
    pub label: String,
    /// Node type
    pub node_type: NodeType,
    /// Path to the source file (if applicable)
    pub file_path: Option<PathBuf>,
    /// Description from YAML schema
    pub description: Option<String>,
}

impl NodeData {
    /// Display name with type prefix for non-model nodes
    pub fn display_name(&self) -> String {
        let prefix = self.node_type.prefix();
        if prefix.is_empty() {
            self.label.clone()
        } else {
            format!("{}{}", prefix, self.label)
        }
    }
}

/// Edge types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum EdgeType {
    /// ref() dependency
    Ref,
    /// source() dependency
    Source,
    /// Test relationship
    Test,
    /// Exposure dependency
    Exposure,
}

/// Data associated with each edge
#[derive(Debug, Clone)]
pub struct EdgeData {
    pub edge_type: EdgeType,
}
