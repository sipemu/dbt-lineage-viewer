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
    /// Declared type; enforced when the model has `contract: {enforced: true}`.
    #[serde(default)]
    pub data_type: Option<String>,
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
    #[serde(default)]
    pub config: Option<ModelConfig>,
    #[serde(default, deserialize_with = "string_or_seq")]
    pub tags: Vec<String>,
    /// Model governance (dbt 1.5+): `private`, `protected` or `public`.
    #[serde(default)]
    pub access: Option<String>,
    #[serde(default)]
    pub group: Option<String>,
    /// Model versions (`versions: [{v: 1}, {v: 2}]`).
    #[serde(default)]
    pub versions: Vec<ModelVersion>,
    /// Version an unversioned `ref()` resolves to; dbt defaults to the
    /// highest.
    #[serde(default)]
    pub latest_version: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelVersion {
    /// Version identifier; numbers and strings are both allowed by dbt.
    pub v: serde_yaml::Value,
    /// File (without `.sql`) defining this version; default `<model>_v<v>`.
    #[serde(default)]
    pub defined_in: Option<String>,
}

/// A version identifier as dbt writes it in unique ids (`2`, `2.1`, `beta`).
pub fn version_string(v: &serde_yaml::Value) -> String {
    match v {
        serde_yaml::Value::String(s) => s.clone(),
        serde_yaml::Value::Number(n) => n.to_string(),
        other => serde_yaml::to_string(other)
            .unwrap_or_default()
            .trim()
            .to_string(),
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ContractConfig {
    #[serde(default)]
    pub enforced: bool,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ModelConfig {
    #[serde(default)]
    pub materialized: Option<String>,
    #[serde(default, deserialize_with = "string_or_seq")]
    pub tags: Vec<String>,
    #[serde(default)]
    pub contract: Option<ContractConfig>,
    #[serde(default)]
    pub access: Option<String>,
    #[serde(default)]
    pub group: Option<String>,
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
/// dbt accepts `tags: foo` as well as `tags: [foo, bar]`.
fn string_or_seq<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
        Null(()),
    }
    Ok(match OneOrMany::deserialize(deserializer)? {
        OneOrMany::One(s) => vec![s],
        OneOrMany::Many(v) => v,
        OneOrMany::Null(()) => Vec::new(),
    })
}

pub fn parse_schema_file(content: &str) -> Result<SchemaFile, serde_yaml::Error> {
    // serde_yaml resolves anchors/aliases but not `<<: *anchor` merge keys,
    // which dbt projects use to share config blocks between models.
    let mut value: serde_yaml::Value = match serde_yaml::from_str(content) {
        Ok(v) => v,
        // dbt's PyYAML loader accepts duplicate keys (last one wins); serde_yaml
        // rejects the whole file. Retry with a lenient loader before giving up.
        Err(e) => lenient_yaml(content).ok_or(e)?,
    };
    value.apply_merge()?;
    serde_yaml::from_value(value)
}

/// Build a `serde_yaml::Value` from yaml-rust2 parser events, PyYAML-style:
/// duplicate keys keep the last value, aliases resolve to their anchors.
pub(crate) fn lenient_yaml(content: &str) -> Option<serde_yaml::Value> {
    use serde_yaml::{Mapping, Value};
    use std::collections::HashMap;
    use yaml_rust2::parser::{Event, MarkedEventReceiver, Parser};
    use yaml_rust2::scanner::{Marker, TScalarStyle};

    enum Frame {
        Seq(Vec<Value>, usize),
        Map(Mapping, Option<Value>, usize),
    }
    #[derive(Default)]
    struct Builder {
        stack: Vec<Frame>,
        anchors: HashMap<usize, Value>,
        root: Option<Value>,
    }
    impl Builder {
        fn push_value(&mut self, v: Value, anchor: usize) {
            if anchor > 0 {
                self.anchors.insert(anchor, v.clone());
            }
            match self.stack.last_mut() {
                Some(Frame::Seq(items, _)) => items.push(v),
                Some(Frame::Map(map, key, _)) => match key.take() {
                    Some(k) => {
                        map.insert(k, v);
                    }
                    None => *key = Some(v),
                },
                None => {
                    if self.root.is_none() {
                        self.root = Some(v);
                    }
                }
            }
        }
    }
    impl MarkedEventReceiver for Builder {
        fn on_event(&mut self, ev: Event, _mark: Marker) {
            match ev {
                Event::Scalar(text, style, anchor, _) => {
                    let v = if style == TScalarStyle::Plain {
                        // Plain scalars carry YAML types (null, bool, numbers).
                        serde_yaml::from_str(&text).unwrap_or(Value::String(text))
                    } else {
                        Value::String(text)
                    };
                    self.push_value(v, anchor);
                }
                Event::Alias(id) => {
                    let v = self.anchors.get(&id).cloned().unwrap_or(Value::Null);
                    self.push_value(v, 0);
                }
                Event::SequenceStart(anchor, _) => self.stack.push(Frame::Seq(Vec::new(), anchor)),
                Event::MappingStart(anchor, _) => {
                    self.stack.push(Frame::Map(Mapping::new(), None, anchor))
                }
                Event::SequenceEnd | Event::MappingEnd => match self.stack.pop() {
                    Some(Frame::Seq(items, anchor)) => {
                        self.push_value(Value::Sequence(items), anchor)
                    }
                    Some(Frame::Map(map, _, anchor)) => {
                        self.push_value(Value::Mapping(map), anchor)
                    }
                    None => {}
                },
                _ => {}
            }
        }
    }

    let mut builder = Builder::default();
    Parser::new_from_str(content)
        .load(&mut builder, false)
        .ok()?;
    builder.root
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tags_as_string_and_duplicate_keys() {
        let yaml = r#"
version: 2
models:
  - name: a
    description: first
    description: second
    tags: input_layer
    config:
      tags: core
      materialized: view
"#;
        let schema = parse_schema_file(yaml).unwrap();
        let a = &schema.models[0];
        assert_eq!(a.description.as_deref(), Some("second"));
        assert_eq!(a.tags, vec!["input_layer".to_string()]);
        let config = a.config.as_ref().unwrap();
        assert_eq!(config.tags, vec!["core".to_string()]);
        assert_eq!(config.materialized.as_deref(), Some("view"));
    }

    #[test]
    fn test_merge_keys_are_applied() {
        let yaml = r#"
version: 2
models:
  - name: a
    config: &shared
      materialized: table
      tags: [core]
  - name: b
    config:
      <<: *shared
      alias: bee
"#;
        let schema = parse_schema_file(yaml).unwrap();
        let b = schema.models.iter().find(|m| m.name == "b").unwrap();
        let config = format!("{:?}", b);
        assert!(config.contains("table"), "{config}");
        assert!(config.contains("core"), "{config}");
    }

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
