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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_all_variants() {
        assert_eq!(NodeType::Model.prefix(), "");
        assert_eq!(NodeType::Source.prefix(), "src:");
        assert_eq!(NodeType::Seed.prefix(), "seed:");
        assert_eq!(NodeType::Snapshot.prefix(), "snap:");
        assert_eq!(NodeType::Test.prefix(), "test:");
        assert_eq!(NodeType::Exposure.prefix(), "exp:");
        assert_eq!(NodeType::Phantom.prefix(), "?:");
    }

    #[test]
    fn test_label_all_variants() {
        assert_eq!(NodeType::Model.label(), "model");
        assert_eq!(NodeType::Source.label(), "source");
        assert_eq!(NodeType::Seed.label(), "seed");
        assert_eq!(NodeType::Snapshot.label(), "snapshot");
        assert_eq!(NodeType::Test.label(), "test");
        assert_eq!(NodeType::Exposure.label(), "exposure");
        assert_eq!(NodeType::Phantom.label(), "phantom");
    }

    #[test]
    fn test_display_name_model() {
        let node = NodeData {
            unique_id: "model.orders".into(),
            label: "orders".into(),
            node_type: NodeType::Model,
            file_path: None,
            description: None,
        };
        assert_eq!(node.display_name(), "orders");
    }

    #[test]
    fn test_display_name_source() {
        let node = NodeData {
            unique_id: "source.raw.orders".into(),
            label: "raw.orders".into(),
            node_type: NodeType::Source,
            file_path: None,
            description: None,
        };
        assert_eq!(node.display_name(), "src:raw.orders");
    }

    #[test]
    fn test_display_name_all_prefixed_types() {
        let types = [
            (NodeType::Seed, "seed:x"),
            (NodeType::Snapshot, "snap:x"),
            (NodeType::Test, "test:x"),
            (NodeType::Exposure, "exp:x"),
            (NodeType::Phantom, "?:x"),
        ];
        for (nt, expected) in types {
            let node = NodeData {
                unique_id: "id".into(),
                label: "x".into(),
                node_type: nt,
                file_path: None,
                description: None,
            };
            assert_eq!(node.display_name(), expected, "Failed for {:?}", nt);
        }
    }
}
