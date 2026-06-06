//! MCP `prompts/list` and `prompts/get` support. Predefined workflow templates
//! that MCP clients render as slash-commands or buttons.

use anyhow::{anyhow, Result};
use serde_json::{json, Value};

pub struct PromptSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub arguments: Value,
}

pub fn registry() -> Vec<PromptSpec> {
    vec![
        PromptSpec {
            name: "review-impact",
            description: "Run impact analysis for a model and walk through the blast radius, calling out tests to add for the most critical downstream paths.",
            arguments: json!([
                {"name": "model", "description": "Model to analyze", "required": true}
            ]),
        },
        PromptSpec {
            name: "propose-tests",
            description: "Inspect a model's SQL and column types and propose generic tests (not_null/unique/relationships/accepted_values).",
            arguments: json!([
                {"name": "model", "description": "Model to propose tests for", "required": true}
            ]),
        },
        PromptSpec {
            name: "explain-column",
            description: "Trace a column back upstream and narrate where each value comes from, including transformations.",
            arguments: json!([
                {"name": "model", "description": "Model owning the column", "required": true},
                {"name": "column", "description": "Column to explain", "required": true}
            ]),
        },
        PromptSpec {
            name: "find-bottleneck",
            description: "Use the critical-path runtime data from target/run_results.json (if present) to find the slowest model on the path to the focus model.",
            arguments: json!([
                {"name": "model", "description": "Model to trace back from", "required": true}
            ]),
        },
        PromptSpec {
            name: "document-model",
            description: "Generate Markdown documentation for a model with description, columns, and lineage diagram.",
            arguments: json!([
                {"name": "model", "description": "Model to document", "required": true}
            ]),
        },
        PromptSpec {
            name: "onboard-project",
            description: "Tour an unfamiliar dbt project: summary, top fan-out models, untested marts, and suggested next steps.",
            arguments: json!([]),
        },
    ]
}

/// Render a prompt name + arguments into MCP's `messages` shape.
pub fn get(name: &str, args: &Value) -> Result<Value> {
    let text = match name {
        "review-impact" => {
            let model = required_str(args, "model")?;
            format!(
                "Use the `impact` tool to compute the blast radius of changing model `{model}`. Walk through each critical / high-severity downstream node, explaining what would break and which tests would catch the regression. End with a checklist of tests to add."
            )
        }
        "propose-tests" => {
            let model = required_str(args, "model")?;
            format!(
                "Read `{model}` via `read_model_sql` and `get_model_details`. For each output column, suggest the most relevant generic test (not_null / unique / accepted_values / relationships). For each suggestion, call `propose_test` to draft a YAML snippet."
            )
        }
        "explain-column" => {
            let model = required_str(args, "model")?;
            let column = required_str(args, "column")?;
            format!(
                "Use `column_upstream(model=\"{model}\", column=\"{column}\")` to trace the column upstream. Read each step's SQL via `read_model_sql` to identify the transformation. Narrate the path source → mart in plain language for an analytics engineer reviewing this column."
            )
        }
        "find-bottleneck" => {
            let model = required_str(args, "model")?;
            format!(
                "Use `lineage_bundle(model=\"{model}\", upstream=10, downstream=0)` to gather the upstream chain. Then identify the slowest hop on the critical path. If `target/run_results.json` exists, refer to its execution times. Report the bottleneck and explain why it dominates."
            )
        }
        "document-model" => {
            let model = required_str(args, "model")?;
            format!(
                "Use `get_model_details(model=\"{model}\")` and `read_model_sql(model=\"{model}\")` to draft a Markdown documentation page. Include: description (or one inferred from the SQL), upstream / downstream lists, columns with brief inferred descriptions, and a Mermaid lineage block (use the `dbt-lineage://model/{model}/lineage.mermaid` resource)."
            )
        }
        "onboard-project" => {
            "Call `summary` to get an overview. Then surface: top 3 fan-out models, any untested marts (use `search_models` + `impact` to identify them), and suggest the first 3 things a new contributor should read. Keep it to one screen."
                .to_string()
        }
        other => return Err(anyhow!("unknown prompt: {}", other)),
    };

    Ok(json!({
        "description": format!("dbt-lineage prompt: {}", name),
        "messages": [{
            "role": "user",
            "content": {"type": "text", "text": text}
        }]
    }))
}

fn required_str<'a>(args: &'a Value, key: &str) -> Result<&'a str> {
    args.get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing required argument: '{}'", key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_lists_all_prompts() {
        let names: Vec<&str> = registry().iter().map(|p| p.name).collect();
        for n in [
            "review-impact",
            "propose-tests",
            "explain-column",
            "find-bottleneck",
            "document-model",
            "onboard-project",
        ] {
            assert!(names.contains(&n), "missing prompt {}", n);
        }
    }

    #[test]
    fn test_get_review_impact_includes_model() {
        let v = get("review-impact", &json!({"model": "orders"})).unwrap();
        let text = v["messages"][0]["content"]["text"].as_str().unwrap();
        assert!(text.contains("orders"));
    }

    #[test]
    fn test_get_explain_column_requires_both_args() {
        let err = get("explain-column", &json!({"model": "orders"})).unwrap_err();
        assert!(err.to_string().contains("column"));
    }

    #[test]
    fn test_get_unknown_prompt_errors() {
        let err = get("bogus", &json!({})).unwrap_err();
        assert!(err.to_string().contains("unknown prompt"));
    }
}
