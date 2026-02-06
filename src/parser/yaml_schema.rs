use serde::Deserialize;

/// Top-level schema YAML file (can contain sources, models, exposures)
#[derive(Debug, Deserialize, Default)]
pub struct SchemaFile {
    #[serde(default)]
    pub sources: Vec<SourceDefinition>,

    #[serde(default)]
    pub models: Vec<ModelDefinition>,

    #[serde(default)]
    pub exposures: Vec<ExposureDefinition>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SourceDefinition {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tables: Vec<SourceTable>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SourceTable {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tests: Vec<TestDefinition>,
}

/// Tests can be either a string or a map
#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum TestDefinition {
    Simple(String),
    Complex(serde_yaml::Value),
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelDefinition {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExposureDefinition {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "type", default)]
    pub exposure_type: Option<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub owner: Option<ExposureOwner>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExposureOwner {
    pub name: Option<String>,
    pub email: Option<String>,
}

/// Parse a schema YAML file
pub fn parse_schema_file(content: &str) -> Result<SchemaFile, serde_yaml::Error> {
    serde_yaml::from_str(content)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sources() {
        let yaml = r#"
sources:
  - name: raw
    description: Raw data from the warehouse
    tables:
      - name: orders
        description: Raw orders table
      - name: customers
"#;
        let schema = parse_schema_file(yaml).unwrap();
        assert_eq!(schema.sources.len(), 1);
        assert_eq!(schema.sources[0].name, "raw");
        assert_eq!(schema.sources[0].tables.len(), 2);
        assert_eq!(schema.sources[0].tables[0].name, "orders");
    }

    #[test]
    fn test_parse_models() {
        let yaml = r#"
models:
  - name: stg_orders
    description: Staged orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
"#;
        let schema = parse_schema_file(yaml).unwrap();
        assert_eq!(schema.models.len(), 1);
        assert_eq!(schema.models[0].name, "stg_orders");
        assert_eq!(schema.models[0].columns.len(), 1);
    }

    #[test]
    fn test_parse_exposures() {
        let yaml = r#"
exposures:
  - name: weekly_report
    description: Weekly business report
    type: dashboard
    depends_on:
      - ref('orders')
      - ref('customers')
    owner:
      name: Data Team
      email: data@example.com
"#;
        let schema = parse_schema_file(yaml).unwrap();
        assert_eq!(schema.exposures.len(), 1);
        assert_eq!(schema.exposures[0].name, "weekly_report");
        assert_eq!(schema.exposures[0].depends_on.len(), 2);
    }

    #[test]
    fn test_empty_file() {
        let yaml = "";
        let schema = parse_schema_file(yaml).unwrap();
        assert!(schema.sources.is_empty());
        assert!(schema.models.is_empty());
        assert!(schema.exposures.is_empty());
    }
}
