//! `ref()` / `source()` calls inside Jinja for-loops over literal lists:
//!
//! ```text
//! {% for t in ['agency', 'stops'] %} select * from {{ source('gtfs', t) }} {% endfor %}
//!
//! {% set models = [] %}
//! {% do models.append(('stg_sent', 'SENT')) if var('sent_enabled') %}
//! {% for model, event_type in models %} select * from {{ ref(model) }} {% endfor %}
//! ```
//!
//! Each list item yields one call. Conditional `append`s count as if enabled:
//! for impact analysis, a dependency that is switched off by a var is still a
//! dependency of the code.

use regex::Regex;
use std::collections::HashMap;
use std::sync::LazyLock;

use super::sql::{RefCall, SourceCall, JINJA_BLOCK};

/// One list item: a string, or a tuple of strings.
type Item = Vec<String>;

static STRING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"'([^']*)'|"([^"]*)""#).unwrap());

static SET_LIST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)^\{%-?\s*set\s+([A-Za-z_]\w*)\s*=\s*\[(.*)\]\s*-?%\}$").unwrap()
});

static APPEND: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)^\{%-?\s*do\s+([A-Za-z_]\w*)\.append\(\s*(.*?)\s*\)(?:\s+if\s.*)?\s*-?%\}$")
        .unwrap()
});

static FOR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)^\{%-?\s*for\s+\(?\s*([A-Za-z_][\w\s,]*?)\s*\)?\s+in\s+(.*?)\s*-?%\}$")
        .unwrap()
});

static ENDFOR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\{%-?\s*endfor\b").unwrap());

const ARG: &str = r#"'[^']*'|"[^"]*"|[A-Za-z_][A-Za-z0-9_]*"#;

static REF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"\bref\s*\(\s*({ARG})\s*(?:,\s*({ARG})\s*)?(?:,\s*(?:v|version)\s*=[^)]*)?\)"
    ))
    .unwrap()
});

static SOURCE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!(r"\bsource\s*\(\s*({ARG})\s*,\s*({ARG})\s*\)")).unwrap());

/// Items of a literal list body such as `'a', 'b'` or `('a', 'A'), ('b', 'B')`.
/// `None` when anything other than string literals and tuples of them appears.
fn parse_items(list: &str) -> Option<Vec<Item>> {
    let mut items = Vec::new();
    let mut rest = list.trim();
    while !rest.is_empty() {
        let item_text;
        if let Some(inner) = rest.strip_prefix('(') {
            let end = inner.find(')')?;
            item_text = &inner[..end];
            rest = inner[end + 1..].trim_start();
        } else {
            let end = rest.find(',').unwrap_or(rest.len());
            item_text = &rest[..end];
            rest = &rest[end..];
        }
        let strings: Vec<String> = STRING
            .captures_iter(item_text)
            .map(|c| c.get(1).or(c.get(2)).unwrap().as_str().to_string())
            .collect();
        let leftover = STRING.replace_all(item_text, "");
        if strings.is_empty()
            || leftover
                .chars()
                .any(|c| !matches!(c, ',' | ' ' | '\n' | '\t' | '\r'))
        {
            return None;
        }
        items.push(strings);
        rest = rest
            .trim_start()
            .strip_prefix(',')
            .unwrap_or(rest)
            .trim_start();
    }
    Some(items)
}

fn resolve(arg: &str, bound: &HashMap<&str, &str>) -> Option<String> {
    let arg = arg.trim();
    if arg.len() >= 2 && (arg.starts_with('\'') || arg.starts_with('"')) {
        return Some(arg[1..arg.len() - 1].to_string());
    }
    bound.get(arg).map(|s| s.to_string())
}

/// Refs and sources produced by for-loops over literal lists in `sql`.
pub fn extract_loop_calls(sql: &str) -> (Vec<RefCall>, Vec<SourceCall>) {
    let cleaned = super::lexer::strip_non_code(sql);
    let blocks: Vec<&str> = JINJA_BLOCK
        .find_iter(&cleaned)
        .map(|m| m.as_str())
        .collect();
    let mut lists: HashMap<String, Vec<Item>> = HashMap::new();
    let mut refs = Vec::new();
    let mut sources = Vec::new();

    for (i, block) in blocks.iter().enumerate() {
        if let Some(c) = SET_LIST.captures(block) {
            if let Some(items) = parse_items(&c[2]) {
                lists.insert(c[1].to_string(), items);
            }
            continue;
        }
        if let Some(c) = APPEND.captures(block) {
            if let (Some(list), Some(mut items)) = (lists.get_mut(&c[1]), parse_items(&c[2])) {
                list.append(&mut items);
            }
            continue;
        }
        let Some(c) = FOR.captures(block) else {
            continue;
        };
        let targets: Vec<&str> = c[1].split(',').map(str::trim).collect();
        let iterable = c[2].trim();
        let items = match iterable.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            Some(list) => parse_items(list),
            None => lists.get(iterable).cloned(),
        };
        let Some(items) = items else {
            continue;
        };
        // The loop body: blocks up to the matching endfor.
        let mut depth = 1;
        let mut body = Vec::new();
        for b in &blocks[i + 1..] {
            if FOR.is_match(b) {
                depth += 1;
            } else if ENDFOR.is_match(b) {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            body.push(*b);
        }
        for item in &items {
            if item.len() != targets.len() && targets.len() != 1 {
                continue;
            }
            let bound: HashMap<&str, &str> = if targets.len() == 1 && item.len() == 1 {
                HashMap::from([(targets[0], item[0].as_str())])
            } else {
                targets
                    .iter()
                    .copied()
                    .zip(item.iter().map(String::as_str))
                    .collect()
            };
            for b in &body {
                for r in REF.captures_iter(b) {
                    // Only calls that use a loop variable; literal calls are
                    // found by the plain extractor already.
                    let uses_loop_var =
                        |m: Option<regex::Match>| m.is_some_and(|m| bound.contains_key(m.as_str()));
                    if !uses_loop_var(r.get(1)) && !uses_loop_var(r.get(2)) {
                        continue;
                    }
                    let first = resolve(&r[1], &bound);
                    let second = r.get(2).and_then(|m| resolve(m.as_str(), &bound));
                    match (first, second, r.get(2).is_some()) {
                        (Some(package), Some(name), true) => refs.push(RefCall {
                            package: Some(package),
                            name,
                            version: None,
                        }),
                        (Some(name), _, false) => refs.push(RefCall {
                            package: None,
                            name,
                            version: None,
                        }),
                        _ => {}
                    }
                }
                for s in SOURCE.captures_iter(b) {
                    if !bound.contains_key(s[1].trim()) && !bound.contains_key(s[2].trim()) {
                        continue;
                    }
                    if let (Some(source_name), Some(table_name)) =
                        (resolve(&s[1], &bound), resolve(&s[2], &bound))
                    {
                        sources.push(SourceCall {
                            source_name,
                            table_name,
                        });
                    }
                }
            }
        }
    }
    (refs, sources)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ref_names(sql: &str) -> Vec<String> {
        extract_loop_calls(sql)
            .0
            .into_iter()
            .map(|r| r.name)
            .collect()
    }

    #[test]
    fn literal_list_loop_over_sources() {
        let sql = r#"{% for t in [
              "agency_txt",
              "stops_txt",
            ] %}
            select * from {{ source('gtfs' , t) }}
            {% if not loop.last %} union all {% endif %}
            {% endfor %}"#;
        let (_, sources) = extract_loop_calls(sql);
        let tables: Vec<_> = sources.iter().map(|s| s.table_name.as_str()).collect();
        assert_eq!(tables, ["agency_txt", "stops_txt"]);
        assert!(sources.iter().all(|s| s.source_name == "gtfs"));
    }

    #[test]
    fn set_list_then_loop() {
        let sql = "{% set models = ['a', 'b'] %}{% for m in models %}{{ ref(m) }}{% endfor %}";
        assert_eq!(ref_names(sql), ["a", "b"]);
    }

    #[test]
    fn appended_tuples_with_unpacking() {
        let sql = r"{% set ms = [] %}
            {% do ms.append(('sent', 'SENT')) if var('x') %}
            {% do ms.append(('opens', 'OPEN')) %}
            {% for model, event_type in ms %}select '{{ event_type }}' from {{ ref(model) }}{% endfor %}";
        assert_eq!(ref_names(sql), ["sent", "opens"]);
    }

    #[test]
    fn literal_refs_in_loop_body_are_not_duplicated() {
        let sql = "{% for t in ['a', 'b'] %}{{ ref('fixed') }}{% endfor %}";
        assert!(ref_names(sql).is_empty());
    }

    #[test]
    fn computed_list_is_skipped() {
        let sql = "{% for m in var('models') %}{{ ref(m) }}{% endfor %}";
        assert!(ref_names(sql).is_empty());
        let sql = "{% set ms = get_models() %}{% for m in ms %}{{ ref(m) }}{% endfor %}";
        assert!(ref_names(sql).is_empty());
    }

    #[test]
    fn nested_loops_bind_their_own_variables() {
        let sql = "{% for s in ['x'] %}{% for t in ['a', 'b'] %}{{ source(s, t) }}{% endfor %}{% endfor %}";
        // The inner loop resolves `t`; `s` is bound by the outer loop only, so
        // the inner pass cannot resolve it and the outer pass cannot resolve `t`.
        assert!(extract_loop_calls(sql).1.is_empty());
    }
}

static CALL_OPEN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b(ref|source)\s*\(").unwrap());
static LITERAL_ARG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"^\s*(?:'[^']*'|"[^"]*"|(?:v|version)\s*=.*)\s*$"#).unwrap());
static IDENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[A-Za-z_]\w*$").unwrap());

/// `ref()` / `source()` calls whose arguments are computed at render time
/// (`ref(var('model'))`, `source(src, table)`) and that no for-loop over a
/// literal list resolves. Parsing can't know what they point to.
pub fn unresolved_dynamic_calls(sql: &str) -> Vec<String> {
    let cleaned = super::lexer::strip_non_code(sql);
    let blocks: Vec<&str> = JINJA_BLOCK
        .find_iter(&cleaned)
        .map(|m| m.as_str())
        .collect();

    // Literal lists, and for each block the loop variables bound to them.
    let mut lists: HashMap<String, Vec<Item>> = HashMap::new();
    let mut bound: Vec<Vec<String>> = vec![Vec::new(); blocks.len()];
    for (i, block) in blocks.iter().enumerate() {
        if let Some(c) = SET_LIST.captures(block) {
            if let Some(items) = parse_items(&c[2]) {
                lists.insert(c[1].to_string(), items);
            }
            continue;
        }
        if let Some(c) = APPEND.captures(block) {
            if let (Some(list), Some(mut items)) = (lists.get_mut(&c[1]), parse_items(&c[2])) {
                list.append(&mut items);
            }
            continue;
        }
        let Some(c) = FOR.captures(block) else {
            continue;
        };
        let iterable = c[2].trim();
        let literal = iterable
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .is_some_and(|l| parse_items(l).is_some())
            || lists.contains_key(iterable);
        if !literal {
            continue;
        }
        let targets: Vec<String> = c[1].split(',').map(|t| t.trim().to_string()).collect();
        let mut depth = 1;
        for (j, b) in blocks.iter().enumerate().skip(i + 1) {
            if FOR.is_match(b) {
                depth += 1;
            } else if ENDFOR.is_match(b) {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            bound[j].extend(targets.iter().cloned());
        }
    }

    let mut out = Vec::new();
    for (i, block) in blocks.iter().enumerate() {
        for m in CALL_OPEN.find_iter(block) {
            let Some(args) = super::macros::call_args(block, m.end()) else {
                continue;
            };
            let parts: Vec<&str> = args.split(',').collect();
            let dynamic: Vec<&str> = parts
                .iter()
                .map(|a| a.trim())
                .filter(|a| !LITERAL_ARG.is_match(a))
                .collect();
            if dynamic.is_empty() {
                continue;
            }
            let resolved = dynamic
                .iter()
                .all(|a| IDENT.is_match(a) && bound[i].iter().any(|v| v == a));
            if !resolved {
                out.push(format!(
                    "{}({})",
                    m.as_str()[..m.as_str().len() - 1].trim_end(),
                    args.trim()
                ));
            }
        }
    }
    out
}

#[cfg(test)]
mod dynamic_tests {
    use super::unresolved_dynamic_calls;

    #[test]
    fn literal_calls_are_not_dynamic() {
        let sql = "select * from {{ ref('a') }} join {{ ref('pkg', 'b', v=2) }} join {{ source('s', 't') }}";
        assert!(unresolved_dynamic_calls(sql).is_empty());
    }

    #[test]
    fn computed_arguments_are_reported() {
        let sql =
            "select * from {{ ref(var('orders_model')) }} join {{ source(src, 'customers') }}";
        assert_eq!(
            unresolved_dynamic_calls(sql),
            ["ref(var('orders_model'))", "source(src, 'customers')"]
        );
    }

    #[test]
    fn calls_resolved_by_literal_loops_are_not_reported() {
        let sql = "{% for m in ['a', 'b'] %}select * from {{ ref(m) }}{% endfor %}";
        assert!(unresolved_dynamic_calls(sql).is_empty());
        let sql = "{% for m in var('models') %}select * from {{ ref(m) }}{% endfor %}";
        assert_eq!(unresolved_dynamic_calls(sql), ["ref(m)"]);
    }

    #[test]
    fn calls_in_comments_are_ignored() {
        assert!(unresolved_dynamic_calls("-- {{ ref(x) }}\n{# ref(y) #}").is_empty());
    }
}
