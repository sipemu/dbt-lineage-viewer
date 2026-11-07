//! Approximate `dbt compile` for column lineage: renders a model's Jinja with
//! the project's and installed packages' macros, so SELECTs written by macros
//! and loops (Fivetran's `fill_staging_columns`, `dbt_utils.star`, …) become
//! plain SQL the AST analyzer can read.
//!
//! Nothing touches a warehouse. `ref()` / `source()` render to the relation
//! placeholders the analyzer resolves; `adapter.get_columns_in_relation()`
//! returns the columns the analyzer already knows for that relation (YAML
//! declarations, seed headers, upstream models' inferred columns);
//! `run_query()` returns an empty table; `is_incremental()` is false; `var()`
//! reads the project's and packages' `vars`; `env_var()` its default.
//! dbt's own adapter macros (`dbt.type_string()`, `dbt.dateadd()`, …) are
//! replaced by small DuckDB-flavoured stand-ins.
//!
//! Each macro file is a template whose macros are renamed `__raw__<name>`;
//! every macro name is also a global that resolves dbt's way (the caller's
//! package, the root project, any package, dbt) and runs the macro in a state
//! of its own, so `return()` can hand back a value instead of text. Lists and
//! dicts bound with `{% set %}` become mutable so `.append()` / `.update()`
//! work. Rendering fails (and the caller falls back to its own Jinja handling)
//! on anything unsupported.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};

use minijinja::value::{Enumerator, Kwargs, Object, ObjectRepr, Value};
use minijinja::{Environment, Error, ErrorKind, State, UndefinedBehavior};
use regex::Regex;

use super::project::DbtProject;

/// Prefix of relation placeholders, as `ast_column_lineage` reads them.
const REL_PREFIX: &str = "__dbtl_rel_";
/// Prefix macro definitions are renamed with.
const RAW: &str = "__raw__";
/// Package holding the dbt stand-ins.
const DBT: &str = "dbt";
/// Macro nesting limit.
const MAX_DEPTH: usize = 64;
/// Upper bound on template instructions per macro call or model.
const FUEL: u64 = 2_000_000;

/// SQL rendered from a model, with the node id behind each relation
/// placeholder `__dbtl_rel_<i>__`.
#[derive(Debug, Clone)]
pub struct Rendered {
    pub sql: String,
    pub relations: Vec<String>,
}

/// What a model render needs from the analyzer.
pub struct ModelInput<'a> {
    /// The model's node id (`this`).
    pub node_id: &'a str,
    /// The model's file, relative to the project root (picks its package).
    pub file_path: Option<&'a Path>,
    /// Known columns of relations the model may introspect, by node id.
    pub columns: HashMap<String, Vec<String>>,
}

/// A project's macros, vars and ref names, ready to render models.
pub struct Renderer {
    env: Environment<'static>,
    shared: Arc<Shared>,
}

struct Shared {
    root: String,
    /// package → macro name → template name.
    macros: HashMap<String, HashMap<String, String>>,
    /// Packages in lookup order after the root project.
    package_order: Vec<String>,
    /// Package directory (under `dbt_packages/`) → package name.
    package_dirs: HashMap<String, String>,
    vars: Vars,
    by_ref: HashMap<String, String>,
}

#[derive(Default)]
struct Vars {
    /// Root project vars, global.
    root: BTreeMap<String, Value>,
    /// Root project vars scoped to a package.
    scoped: HashMap<String, BTreeMap<String, Value>>,
    /// Each package's own defaults.
    package: HashMap<String, BTreeMap<String, Value>>,
}

/// Per-render state (renders are single-threaded and not re-entrant).
#[derive(Default)]
struct RenderState {
    relations: Vec<String>,
    columns: HashMap<String, Vec<String>>,
    returns: Vec<Option<Value>>,
    package: String,
    node_id: String,
}

thread_local! {
    static STATE: RefCell<RenderState> = RefCell::new(RenderState::default());
}

fn with_state<R>(f: impl FnOnce(&mut RenderState) -> R) -> R {
    STATE.with(|s| f(&mut s.borrow_mut()))
}

fn err(msg: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidOperation, msg.into())
}

impl Renderer {
    /// Load the macros of the project at `root` and its `dbt_packages/`.
    /// `by_ref` maps ref names to node ids (see `ast_column_lineage`).
    pub fn new(root: &Path, by_ref: HashMap<String, String>) -> Option<Renderer> {
        let project = DbtProject::load(root).ok()?;
        let mut macro_files: Vec<(String, PathBuf, PathBuf)> = Vec::new(); // (pkg, base, file)
        let mut vars = Vars::default();
        let mut package_order = Vec::new();
        let mut package_dirs = HashMap::new();
        let mut packages: Vec<(String, PathBuf, DbtProject)> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(root.join("dbt_packages")) {
            let mut dirs: Vec<PathBuf> = entries.filter_map(|e| e.ok()).map(|e| e.path()).collect();
            dirs.sort();
            for dir in dirs {
                if let Ok(p) = DbtProject::load(&dir) {
                    if let Some(d) = dir.file_name().and_then(|d| d.to_str()) {
                        package_dirs.insert(d.to_string(), p.name.clone());
                    }
                    if p.name != project.name {
                        packages.push((p.name.clone(), dir, p));
                    }
                }
            }
        }
        for path in &project.macro_paths {
            macro_files.extend(
                sql_files(&root.join(path)).map(|f| (project.name.clone(), root.to_path_buf(), f)),
            );
        }
        let package_names: HashSet<String> = packages.iter().map(|(n, _, _)| n.clone()).collect();
        for (name, k, v) in project.vars.iter().map(|(k, v)| (k.clone(), k, v)) {
            match (package_names.contains(&name) || name == project.name, v) {
                (true, serde_yaml::Value::Mapping(m)) => {
                    let scoped = vars.scoped.entry(name).or_default();
                    for (k, v) in m {
                        if let Some(k) = k.as_str() {
                            scoped.insert(k.to_string(), yaml_value(v));
                        }
                    }
                }
                _ => {
                    vars.root.insert(k.clone(), yaml_value(v));
                }
            }
        }
        for (name, dir, p) in packages {
            for path in &p.macro_paths {
                macro_files
                    .extend(sql_files(&dir.join(path)).map(|f| (name.clone(), dir.clone(), f)));
            }
            let defaults = vars.package.entry(name.clone()).or_default();
            for (k, v) in &p.vars {
                defaults.entry(k.clone()).or_insert_with(|| yaml_value(v));
            }
            package_order.push(name);
        }

        let mut env = Environment::new();
        env.set_undefined_behavior(UndefinedBehavior::Chainable);
        env.set_fuel(Some(FUEL));
        env.set_unknown_method_callback(minijinja_contrib::pycompat::unknown_method_callback);
        // Templates compile on first use: most projects call few macros.
        let mut macros: HashMap<String, HashMap<String, String>> = HashMap::new();
        let mut sources: HashMap<String, String> = HashMap::new();
        let mut add = |pkg: &str, name: String, source: &str| {
            let source = prepare_macro_source(source);
            let table = macros.entry(pkg.to_string()).or_default();
            for c in MACRO_DEF.captures_iter(&source) {
                if let Some(m) = c[2].strip_prefix(RAW) {
                    table.entry(m.to_string()).or_insert_with(|| name.clone());
                }
            }
            sources.insert(name, source);
        };
        for (pkg, base, file) in &macro_files {
            let Ok(source) = std::fs::read_to_string(file) else {
                continue;
            };
            let rel = file
                .strip_prefix(base)
                .unwrap_or(file)
                .display()
                .to_string();
            add(pkg, format!("{pkg}/{rel}"), &source);
        }
        env.set_loader(move |name| Ok(sources.get(name).cloned()));

        let shared = Arc::new(Shared {
            root: project.name.clone(),
            macros,
            package_order,
            package_dirs,
            vars,
            by_ref,
        });
        install_globals(&mut env, &shared);
        Some(Renderer { env, shared })
    }

    /// Render a model's SQL. `None` when the template uses anything this
    /// renderer doesn't support.
    pub fn render(&self, sql: &str, input: ModelInput<'_>) -> Option<Rendered> {
        let package = input
            .file_path
            .and_then(|p| package_of(p, &self.shared.package_dirs))
            .unwrap_or_else(|| self.shared.root.clone());
        with_state(|s| {
            *s = RenderState {
                columns: input.columns,
                package,
                node_id: input.node_id.to_string(),
                ..RenderState::default()
            }
        });
        let source = prepare_source(sql);
        let out = self
            .env
            .template_from_str(&source)
            .and_then(|t| t.render(()));
        let relations = with_state(|s| std::mem::take(&mut s.relations));
        with_state(|s| *s = RenderState::default());
        out.ok().map(|sql| Rendered { sql, relations })
    }
}

/// `.sql` files under `dir`, sorted.
fn sql_files(dir: &Path) -> impl Iterator<Item = PathBuf> {
    let mut files: Vec<PathBuf> = walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file() && e.path().extension().is_some_and(|x| x == "sql"))
        .map(|e| e.into_path())
        .collect();
    files.sort();
    files.into_iter()
}

/// The package a model file belongs to: `dbt_packages/<dir>/…` → that
/// package's name.
fn package_of(path: &Path, dirs: &HashMap<String, String>) -> Option<String> {
    let mut parts = path.components().map(|c| c.as_os_str().to_string_lossy());
    while let Some(p) = parts.next() {
        if p == "dbt_packages" {
            return parts.next().and_then(|d| dirs.get(d.as_ref()).cloned());
        }
    }
    None
}

fn yaml_value(v: &serde_yaml::Value) -> Value {
    match v {
        serde_yaml::Value::Null => Value::from(()),
        serde_yaml::Value::Bool(b) => Value::from(*b),
        serde_yaml::Value::Number(n) => n
            .as_i64()
            .map(Value::from)
            .or_else(|| n.as_f64().map(Value::from))
            .unwrap_or_default(),
        serde_yaml::Value::String(s) => Value::from(s.as_str()),
        serde_yaml::Value::Sequence(items) => {
            MutList::value(items.iter().map(yaml_value).collect())
        }
        serde_yaml::Value::Mapping(m) => MutDict::value(
            m.iter()
                .filter_map(|(k, v)| Some((Value::from(k.as_str()?), yaml_value(v))))
                .collect(),
        ),
        serde_yaml::Value::Tagged(t) => yaml_value(&t.value),
    }
}

// ---------------------------------------------------------------------------
// Source preparation

static JINJA_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)\{#.*?#\}").unwrap());
static MACRO_DEF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\{%-?\s*macro\s+)([A-Za-z_][A-Za-z0-9_]*)").unwrap());
/// Blocks minijinja doesn't know and models never call.
static FOREIGN_BLOCK: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?s)\{%-?\s*(test|materialization|snapshot|docs)\b.*?\{%-?\s*end(test|materialization|snapshot|docs)\s*-?%\}",
    )
    .unwrap()
});
static DO_TAG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)\{%(-?)\s*do\s(.*?)(-?)%\}").unwrap());
static SET_ASSIGN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\{%-?\s*set\s+[A-Za-z_][A-Za-z0-9_]*\s*=\s*").unwrap());

/// Drop `{# … #}` comments, keeping their line breaks so error lines match.
fn strip_jinja_comments(sql: &str) -> String {
    JINJA_COMMENT
        .replace_all(sql, |c: &regex::Captures| {
            "\n".repeat(c[0].matches('\n').count())
        })
        .into_owned()
}

/// Model or macro source as minijinja reads it.
fn prepare_source(source: &str) -> String {
    wrap_set_literals(&conditional_do(&strip_jinja_comments(source)))
}

/// `{% do x.append(y) if cond %}` (an `if` without `else`, which minijinja
/// rejects) → `{% if cond %}{% do x.append(y) %}{% endif %}`.
fn conditional_do(source: &str) -> String {
    DO_TAG
        .replace_all(source, |c: &regex::Captures| {
            let body = &c[2];
            match top_level_if(body) {
                Some(i) if !body[i..].contains(" else ") => format!(
                    "{{%{} if {} %}}{{% do {} %}}{{% endif {}%}}",
                    &c[1],
                    body[i + 4..].trim(),
                    body[..i].trim(),
                    &c[3]
                ),
                _ => c[0].to_string(),
            }
        })
        .into_owned()
}

/// Byte index of the first ` if ` outside brackets and strings.
fn top_level_if(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    let b = s.as_bytes();
    for i in 0..b.len() {
        let c = b[i];
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            b'\'' | b'"' => quote = Some(c),
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b' ' if depth == 0 && s[i..].starts_with(" if ") => return Some(i),
            _ => {}
        }
    }
    None
}

/// Rename macro definitions to `__raw__<name>`, drop test / materialization
/// blocks and make `{% set %}` list and dict literals mutable.
fn prepare_macro_source(source: &str) -> String {
    let source = prepare_source(source);
    let source = FOREIGN_BLOCK.replace_all(&source, "");
    let source = late_defaults(&source);
    MACRO_DEF
        .replace_all(&source, format!("${{1}}{RAW}${{2}}"))
        .into_owned()
}

/// Placeholder default for a parameter whose default is computed at call time.
const UNSET: &str = "__dbtl_unset__";
static IDENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[A-Za-z_][A-Za-z0-9_]*").unwrap());
static STRING_LIT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*""#).unwrap());

/// Jinja evaluates a macro parameter's default when the macro is called, so a
/// default may read the parameters before it (`alias=column_dict[key]['alias']`);
/// minijinja evaluates it without them. Such defaults become a placeholder
/// the macro body replaces first: `{% macro m(a, b=a ~ 'x') %}` →
/// `{% macro m(a, b='__dbtl_unset__') %}{% if b == '__dbtl_unset__' %}{% set b = a ~ 'x' %}{% endif %}`.
fn late_defaults(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut last = 0;
    for m in MACRO_DEF.find_iter(source) {
        let start = m.end();
        if start < last {
            continue;
        }
        let rest = &source[start..];
        let Some(open) = rest.find('(').filter(|i| {
            rest[..*i]
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c.is_whitespace())
        }) else {
            continue;
        };
        let Some(close) = matching_close(&rest[open..]).map(|c| open + c) else {
            continue;
        };
        let after = &rest[close + 1..];
        let Some(tag_end) = after.find("%}") else {
            continue;
        };
        let between = after[..tag_end].trim();
        if !(between.is_empty() || between == "-") {
            continue;
        }
        let mut names: Vec<&str> = Vec::new();
        let mut params: Vec<String> = Vec::new();
        let mut late: Vec<(&str, &str)> = Vec::new();
        for param in split_top_level(&rest[open + 1..close]) {
            let Some((name, default)) = param.split_once('=') else {
                names.push(param.trim());
                params.push(param.to_string());
                continue;
            };
            let (name, default) = (name.trim(), default.trim());
            let code = STRING_LIT.replace_all(default, "''");
            if IDENT.find_iter(&code).any(|i| names.contains(&i.as_str())) {
                params.push(format!("{name}='{UNSET}'"));
                late.push((name, default));
            } else {
                params.push(param.to_string());
            }
            names.push(name);
        }
        if late.is_empty() {
            continue;
        }
        let tag_close = close + 1 + tag_end + 2;
        out.push_str(&source[last..start]);
        out.push_str(&rest[..=open]);
        out.push_str(&params.join(","));
        out.push_str(&rest[close..tag_close]);
        for (i, (name, default)) in late.iter().enumerate() {
            let dash = if between == "-" && i + 1 == late.len() {
                "-"
            } else {
                ""
            };
            out.push_str(&format!(
                "{{% if {name} == '{UNSET}' %}}{{% set {name} = {default} %}}{{% endif {dash}%}}"
            ));
        }
        last = start + tag_close;
    }
    out.push_str(&source[last..]);
    out
}

/// `s` split at commas outside brackets and strings.
fn split_top_level(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let (mut depth, mut start) = (0i32, 0);
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (i, c) in s.char_indices() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            '\'' | '"' => quote = Some(c),
            '[' | '{' | '(' => depth += 1,
            ']' | '}' | ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if !s[start..].trim().is_empty() {
        parts.push(&s[start..]);
    }
    parts
}

/// `{% set x = [...] %}` → `{% set x = __list([...]) %}`, and `{...}` →
/// `__dict({...})`, so the value supports `.append()` / `.update()`.
fn wrap_set_literals(source: &str) -> String {
    let mut out = String::with_capacity(source.len() + 64);
    let mut last = 0;
    for m in SET_ASSIGN.find_iter(source) {
        let start = m.end();
        if start < last {
            continue;
        }
        let rest = &source[start..];
        let Some(open) = rest.chars().next().filter(|c| *c == '[' || *c == '{') else {
            // Any other value: lists it yields (`a + b`, a var's list) become
            // mutable too; mutable ones pass through, keeping aliases.
            if let Some(end) = tag_end(rest) {
                out.push_str(&source[last..start]);
                out.push_str("__mut(");
                out.push_str(rest[..end].trim_end());
                out.push_str(") ");
                last = start + end;
            }
            continue;
        };
        let Some(close) = matching_close(rest) else {
            continue;
        };
        let after = rest[close + 1..].trim_start();
        if !(after.starts_with("%}") || after.starts_with("-%}")) {
            continue;
        }
        out.push_str(&source[last..start]);
        out.push_str(if open == '[' { "__list(" } else { "__dict(" });
        out.push_str(&rest[..=close]);
        out.push(')');
        last = start + close + 1;
    }
    out.push_str(&source[last..]);
    out
}

/// Byte index of the `%}` / `-%}` ending the tag `s` is inside, outside
/// brackets and strings.
fn tag_end(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (i, c) in s.char_indices() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            '\'' | '"' => quote = Some(c),
            '[' | '{' | '(' => depth += 1,
            ']' | '}' | ')' => depth -= 1,
            '-' | '%' if depth == 0 => {
                let r = &s[i..];
                if r.starts_with("-%}") || r.starts_with("%}") {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Byte index of the bracket closing the one `s` starts with.
fn matching_close(s: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (i, c) in s.char_indices() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            '\'' | '"' => quote = Some(c),
            '[' | '{' | '(' => depth += 1,
            ']' | '}' | ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Macro resolution and calls

impl Shared {
    fn template(&self, pkg: &str, name: &str) -> Option<&String> {
        self.macros.get(pkg).and_then(|m| m.get(name))
    }

    /// Template defining `name`, looked up dbt's way from `caller`'s package
    /// (`ns`: an explicit namespace).
    fn resolve(&self, ns: Option<&str>, name: &str, caller: &str) -> Option<&String> {
        match ns {
            Some(DBT) => self
                .template(DBT, name)
                .or_else(|| self.template(&self.root, name)),
            Some(ns) => self.template(ns, name).or_else(|| self.template(DBT, name)),
            None => self
                .template(caller, name)
                .or_else(|| self.template(&self.root, name))
                .or_else(|| {
                    self.package_order
                        .iter()
                        .find_map(|p| self.template(p, name))
                })
                .or_else(|| self.template(DBT, name)),
        }
    }

    /// `adapter.dispatch(name, namespace)`: the DuckDB or default
    /// implementation, then the plain name.
    fn dispatch(&self, name: &str, ns: Option<&str>, caller: &str) -> Option<&String> {
        let candidates = [format!("duckdb__{name}"), format!("default__{name}")];
        // dbt's default search order: the root project (overrides), then
        // the namespace.
        let mut order: Vec<&str> = vec![&self.root];
        order.extend(ns);
        order.push(caller);
        order.extend(self.package_order.iter().map(String::as_str));
        order.push(DBT);
        for pkg in order {
            for c in &candidates {
                if let Some(t) = self.template(pkg, c) {
                    return Some(t);
                }
            }
        }
        self.resolve(ns, name, caller)
    }

    fn all_macro_names(&self) -> HashSet<&str> {
        self.macros
            .values()
            .flat_map(|m| m.keys().map(String::as_str))
            .collect()
    }

    /// A var as dbt resolves it for a node of package `model` (the model being
    /// rendered, whichever package's macro asks), then for the calling macro's
    /// package, then any package's default.
    fn var(&self, name: &str, model: &str, caller: &str) -> Option<Value> {
        let v = &self.vars;
        let in_package = |pkg: &str| {
            v.scoped
                .get(pkg)
                .and_then(|m| m.get(name))
                .or_else(|| v.root.get(name))
                .or_else(|| v.package.get(pkg).and_then(|m| m.get(name)))
        };
        in_package(model)
            .or_else(|| in_package(caller))
            .or_else(|| {
                self.package_order
                    .iter()
                    .find_map(|p| v.package.get(p)?.get(name))
            })
            .cloned()
    }
}

/// The package of the template being rendered.
fn caller_package(state: &State) -> String {
    match state.name().split_once('/') {
        Some((pkg, _)) if state.name() != "<string>" => pkg.to_string(),
        _ => with_state(|s| s.package.clone()),
    }
}

/// Run macro `name` from `template` in a state of its own; its `return()`
/// value, else its output.
fn call_macro(state: &State, template: &str, name: &str, args: &[Value]) -> Result<Value, Error> {
    let depth = with_state(|s| {
        s.returns.push(None);
        s.returns.len()
    });
    let result = (|| {
        if depth > MAX_DEPTH {
            return Err(err("macro recursion too deep"));
        }
        let tmpl = state.env().get_template(template)?;
        let mut captured = tmpl.render_captured(())?;
        let raw = format!("{RAW}{name}");
        captured.with_state_mut(|st| {
            let m = st
                .lookup(&raw)
                .ok_or_else(|| Error::new(ErrorKind::UnknownFunction, raw.clone()))?;
            m.call(st, args)
        })
    })();
    let returned = with_state(|s| s.returns.pop().flatten());
    let output = result?;
    Ok(returned.unwrap_or(output))
}

/// A macro by name, optionally in a namespace (`dbt_utils.star`).
#[derive(Debug)]
struct MacroRef {
    shared: SharedRef,
    ns: Option<String>,
    name: String,
    dispatch: bool,
}

/// `Arc<Shared>` with a `Debug` impl for minijinja's object bound.
#[derive(Clone)]
struct SharedRef(Arc<Shared>);

impl fmt::Debug for SharedRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("project")
    }
}

impl Object for MacroRef {
    fn call(self: &Arc<Self>, state: &mut State<'_, '_>, args: &[Value]) -> Result<Value, Error> {
        if !self.dispatch && self.ns.as_deref() == Some(DBT) {
            if let Some(v) = dbt_builtin(&self.name, args) {
                return Ok(v);
            }
        }
        let caller = caller_package(state);
        let shared = &self.shared.0;
        let (template, name) = if self.dispatch {
            let t = shared.dispatch(&self.name, self.ns.as_deref(), &caller);
            let name = t.and_then(|t| {
                let pkg = t.split_once('/').map(|(p, _)| p).unwrap_or_default();
                [
                    format!("duckdb__{}", self.name),
                    format!("default__{}", self.name),
                    self.name.clone(),
                ]
                .into_iter()
                .find(|c| shared.template(pkg, c) == Some(t))
            });
            (t, name)
        } else {
            (
                shared.resolve(self.ns.as_deref(), &self.name, &caller),
                Some(self.name.clone()),
            )
        };
        match (template, name) {
            (Some(t), Some(name)) => call_macro(state, t, &name, args),
            // `dbt_utils.type_string()` and friends forward to dbt's.
            _ => match dbt_builtin(&self.name, args) {
                Some(v) => Ok(v),
                None if self.ns.as_deref() == Some(DBT) => generic_sql_call(&self.name, args),
                None => Err(Error::new(ErrorKind::UnknownFunction, self.name.clone())),
            },
        }
    }
}

/// Stand-ins for dbt's adapter macros (DuckDB flavour): only their shape
/// matters, the columns they read.
fn dbt_builtin(name: &str, args: &[Value]) -> Option<Value> {
    let a = |i: usize, key: &str| arg_str(args, i, key).unwrap_or_default();
    let sql = match name {
        "type_string" | "type_text" => "varchar".to_string(),
        "type_timestamp" => "timestamp".to_string(),
        "type_date" => "date".to_string(),
        "type_int" => "integer".to_string(),
        "type_bigint" => "bigint".to_string(),
        "type_numeric" => "numeric".to_string(),
        "type_float" => "float".to_string(),
        "type_boolean" => "boolean".to_string(),
        "current_timestamp"
        | "current_timestamp_backcompat"
        | "current_timestamp_in_utc_backcompat"
        | "snapshot_get_time" => "current_timestamp".to_string(),
        "dateadd" => format!(
            "date_add({}, interval ({}) {})",
            a(2, "from_date_or_timestamp"),
            a(1, "interval"),
            a(0, "datepart")
        ),
        "datediff" => format!(
            "date_diff('{}', {}, {})",
            a(2, "datepart"),
            a(0, "first_date"),
            a(1, "second_date")
        ),
        "date_trunc" => format!("date_trunc('{}', {})", a(0, "datepart"), a(1, "date")),
        "last_day" => format!("last_day({})", a(0, "date")),
        "safe_cast" => format!("try_cast({} as {})", a(0, "field"), a(1, "type")),
        "cast" => format!("cast({} as {})", a(0, "field"), a(1, "type")),
        "cast_bool_to_text" => format!("cast({} as varchar)", a(0, "field")),
        "concat" => {
            let fields = arg(args, 0, "fields")?;
            let parts: Vec<String> = fields.try_iter().ok()?.map(|v| v.to_string()).collect();
            format!("concat({})", parts.join(", "))
        }
        "hash" => format!("md5(cast({} as varchar))", a(0, "field")),
        "split_part" => format!(
            "split_part({}, {}, {})",
            a(0, "string_text"),
            a(1, "delimiter_text"),
            a(2, "part_number")
        ),
        "replace" => format!(
            "replace({}, {}, {})",
            a(0, "field"),
            a(1, "old_chars"),
            a(2, "new_chars")
        ),
        "length" => format!("length({})", a(0, "expression")),
        "position" => format!(
            "position({} in {})",
            a(0, "substring_text"),
            a(1, "string_text")
        ),
        "right" => format!(
            "right({}, {})",
            a(0, "string_text"),
            a(1, "length_expression")
        ),
        "listagg" => format!(
            "string_agg({}, {})",
            a(0, "measure"),
            arg_str(args, 1, "delimiter_text").unwrap_or_else(|| "','".into())
        ),
        "any_value" | "bool_or" | "array_agg" => format!("{name}({})", a(0, "expression")),
        "escape_single_quotes" => a(0, "expression").replace('\'', "''"),
        "string_literal" => format!("'{}'", a(0, "value")),
        "intersect" | "except" => name.to_string(),
        "date" => format!(
            "make_date({}, {}, {})",
            a(0, "year"),
            a(1, "month"),
            a(2, "day")
        ),
        "generate_series" => format!(
            "select range as generated_number from range(1, {} + 1)",
            a(0, "upper_bound")
        ),
        "get_intervals_between" => return Some(Value::from(1)),
        _ => return None,
    };
    Some(Value::from(sql))
}

/// An unknown `dbt.<name>(…)`: the SQL function of that name over the args.
fn generic_sql_call(name: &str, args: &[Value]) -> Result<Value, Error> {
    let parts: Vec<String> = args
        .iter()
        .filter(|a| !a.is_kwargs())
        .map(|a| a.to_string())
        .collect();
    Ok(Value::from(format!("{name}({})", parts.join(", "))))
}

/// A package namespace: attribute access yields its macros.
#[derive(Debug)]
struct PackageNs {
    shared: SharedRef,
    name: String,
}

impl Object for PackageNs {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        Some(Value::from_object(MacroRef {
            shared: self.shared.clone(),
            ns: Some(self.name.clone()),
            name: key.as_str()?.to_string(),
            dispatch: false,
        }))
    }
}

// ---------------------------------------------------------------------------
// dbt context objects

fn split_kwargs(args: &[Value]) -> (&[Value], Option<Kwargs>) {
    match args.last() {
        Some(last) if last.is_kwargs() => {
            (&args[..args.len() - 1], Kwargs::try_from(last.clone()).ok())
        }
        _ => (args, None),
    }
}

/// Positional argument `i`, else keyword `name`.
fn arg(args: &[Value], i: usize, name: &str) -> Option<Value> {
    let (pos, kwargs) = split_kwargs(args);
    pos.get(i)
        .cloned()
        .or_else(|| kwargs.and_then(|k| k.get::<Option<Value>>(name).ok().flatten()))
        .filter(|v| !v.is_undefined())
}

fn arg_str(args: &[Value], i: usize, name: &str) -> Option<String> {
    arg(args, i, name).map(|v| {
        v.as_str()
            .map(str::to_string)
            .unwrap_or_else(|| v.to_string())
    })
}

/// A relation placeholder for a node.
fn relation(node_id: String) -> Value {
    let index = with_state(|s| {
        s.relations.push(node_id);
        s.relations.len() - 1
    });
    Value::from_object(Relation { index })
}

/// A relation: renders as its placeholder.
#[derive(Debug)]
struct Relation {
    index: usize,
}

impl Relation {
    fn placeholder(&self) -> String {
        format!("{REL_PREFIX}{}__", self.index)
    }
}

impl Object for Relation {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Map
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        Some(match key.as_str()? {
            "identifier" | "name" | "table" => Value::from(self.placeholder()),
            // The placeholder again, so `adapter.get_relation(schema=…)`
            // finds the relation.
            "schema" => Value::from(self.placeholder()),
            "database" => Value::from("db"),
            "type" => Value::from("table"),
            "is_table" => Value::from(true),
            "is_view" | "is_cte" => Value::from(false),
            "metadata" => map([("type", Value::from("DuckDBRelation"))]),
            _ => return None,
        })
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Str(&["database", "schema", "identifier", "metadata"])
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &mut State<'_, '_>,
        method: &str,
        args: &[Value],
    ) -> Result<Value, Error> {
        match method {
            "get" => Ok(self
                .get_value(&args.first().cloned().unwrap_or_default())
                .or_else(|| args.get(1).cloned())
                .unwrap_or_default()),
            "include" | "quote" | "incorporate" | "replace_path" => {
                Ok(Value::from_dyn_object(self.clone()))
            }
            "render" | "render_limited" => Ok(Value::from(self.placeholder())),
            _ => Err(Error::from(ErrorKind::UnknownMethod)),
        }
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.placeholder())
    }
}

/// The node behind a relation value or a rendered placeholder.
fn relation_node(v: &Value) -> Option<String> {
    let index = match v.downcast_object_ref::<Relation>() {
        Some(r) => r.index,
        None => {
            let s = v.as_str()?.trim();
            let tail = s.rsplit('.').next()?.trim_matches('"');
            tail.strip_prefix(REL_PREFIX)?
                .strip_suffix("__")?
                .parse()
                .ok()?
        }
    };
    with_state(|s| s.relations.get(index).cloned())
}

/// A column from `adapter.get_columns_in_relation`.
#[derive(Debug)]
struct Column {
    name: String,
}

impl Object for Column {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        Some(match key.as_str()? {
            "name" | "column" => Value::from(self.name.as_str()),
            "quoted" => Value::from(format!("\"{}\"", self.name)),
            "data_type" | "dtype" => Value::from("varchar"),
            "char_size" | "numeric_precision" | "numeric_scale" => Value::from(()),
            _ => return None,
        })
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &mut State<'_, '_>,
        method: &str,
        _args: &[Value],
    ) -> Result<Value, Error> {
        match method {
            "is_string" => Ok(Value::from(true)),
            "is_number" | "is_numeric" | "is_integer" | "is_float" => Ok(Value::from(false)),
            "string_size" => Ok(Value::from(256)),
            _ => Err(Error::from(ErrorKind::UnknownMethod)),
        }
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

/// The relation's known columns. Unknown columns fail the render: macros
/// would otherwise write SQL for a relation without columns.
fn columns_of(v: &Value) -> Result<Value, Error> {
    let cols = relation_node(v)
        .and_then(|id| with_state(|s| s.columns.get(&id).cloned()))
        .ok_or_else(|| err(format!("columns of {v} unknown")))?;
    Ok(MutList::value(
        cols.into_iter()
            .map(|name| Value::from_object(Column { name }))
            .collect(),
    ))
}

/// A `{key: value}` map.
fn map<const N: usize>(pairs: [(&str, Value); N]) -> Value {
    Value::from(
        pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<BTreeMap<String, Value>>(),
    )
}

/// `adapter`.
#[derive(Debug)]
struct Adapter(SharedRef);

impl Object for Adapter {
    fn call_method(
        self: &Arc<Self>,
        state: &mut State<'_, '_>,
        method: &str,
        args: &[Value],
    ) -> Result<Value, Error> {
        match method {
            "dispatch" => {
                let name = arg_str(args, 0, "macro_name").ok_or_else(|| err("dispatch name"))?;
                let ns = arg_str(args, 1, "macro_namespace");
                Ok(Value::from_object(MacroRef {
                    shared: self.0.clone(),
                    ns,
                    name,
                    dispatch: true,
                }))
            }
            "get_columns_in_relation" => columns_of(&arg(args, 0, "relation").unwrap_or_default()),
            "quote" | "quote_as_configured" => Ok(Value::from(format!(
                "\"{}\"",
                arg_str(args, 0, "identifier").unwrap_or_default()
            ))),
            "type" => Ok(Value::from("duckdb")),
            // A relation named by a placeholder (`source(…).schema`) exists.
            "get_relation" | "load_relation" => Ok(["identifier", "schema", "relation"]
                .iter()
                .enumerate()
                .find_map(|(i, k)| relation_node(&arg(args, 2 - i.min(2), k)?))
                .map(relation)
                .unwrap_or_default()),
            "get_missing_columns" | "list_relations_without_caching" => Ok(MutList::value(vec![])),
            "check_schema_exists" | "already_exists" => Ok(Value::from(true)),
            _ => {
                let _ = state;
                Err(Error::new(
                    ErrorKind::UnknownMethod,
                    format!("adapter.{method}"),
                ))
            }
        }
    }
}

/// `config`: callable (no output) with `.get()` / `.require()`.
#[derive(Debug)]
struct Config;

impl Object for Config {
    fn call(self: &Arc<Self>, _state: &mut State<'_, '_>, _args: &[Value]) -> Result<Value, Error> {
        Ok(Value::from(""))
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &mut State<'_, '_>,
        method: &str,
        args: &[Value],
    ) -> Result<Value, Error> {
        match method {
            "get" | "require" => Ok(arg(args, 1, "default").unwrap_or_default()),
            "set" | "persist_relation_docs" | "persist_column_docs" => Ok(Value::from("")),
            _ => Err(Error::from(ErrorKind::UnknownMethod)),
        }
    }
}

/// `this`: the model being rendered.
#[derive(Debug)]
struct This;

impl Object for This {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Map
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "identifier" | "name" | "table" => Some(Value::from(with_state(|s| {
                s.node_id.rsplit('.').next().unwrap_or_default().to_string()
            }))),
            "schema" => Some(Value::from("main")),
            "database" => Some(Value::from("db")),
            _ => None,
        }
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = with_state(|s| s.node_id.clone());
        write!(f, "{}", relation(id))
    }
}

/// `modules`: `modules.re` only.
#[derive(Debug)]
struct Modules;

impl Object for Modules {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "re" => Some(Value::from_object(ReModule)),
            "datetime" | "pytz" => Some(Value::from_object(Clock)),
            _ => None,
        }
    }
}

/// Python's `re` (`sub`, `search`, `match`, `findall`) over the regex crate.
#[derive(Debug)]
struct ReModule;

impl Object for ReModule {
    fn call_method(
        self: &Arc<Self>,
        _state: &mut State<'_, '_>,
        method: &str,
        args: &[Value],
    ) -> Result<Value, Error> {
        let pattern = arg_str(args, 0, "pattern").unwrap_or_default();
        let re = Regex::new(&pattern).map_err(|e| err(e.to_string()))?;
        let text = |i: usize, name: &str| arg_str(args, i, name).unwrap_or_default();
        match method {
            "sub" => {
                let repl = PY_GROUP
                    .replace_all(&text(1, "repl"), "$${$1}")
                    .into_owned();
                Ok(Value::from(
                    re.replace_all(&text(2, "string"), repl.as_str())
                        .into_owned(),
                ))
            }
            "search" | "match" => {
                let s = text(1, "string");
                let m = re.find(&s).filter(|m| method == "search" || m.start() == 0);
                Ok(m.map(|m| Value::from(m.as_str())).unwrap_or_default())
            }
            "findall" => {
                let s = text(1, "string");
                Ok(Value::from(
                    re.find_iter(&s)
                        .map(|m| Value::from(m.as_str()))
                        .collect::<Vec<_>>(),
                ))
            }
            _ => Err(Error::from(ErrorKind::UnknownMethod)),
        }
    }
}

static PY_GROUP: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\\(\d+)").unwrap());

/// `exceptions`.
#[derive(Debug)]
struct Exceptions;

impl Object for Exceptions {
    fn call_method(
        self: &Arc<Self>,
        _state: &mut State<'_, '_>,
        method: &str,
        args: &[Value],
    ) -> Result<Value, Error> {
        match method {
            "warn" => Ok(Value::from("")),
            _ => Err(err(format!(
                "{method}: {}",
                arg_str(args, 0, "msg").unwrap_or_default()
            ))),
        }
    }
}

/// `run_query()` / `load_result()['table']`: an empty agate table.
#[derive(Debug)]
struct EmptyTable;

impl Object for EmptyTable {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Seq
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Empty
    }

    fn is_true(self: &Arc<Self>) -> bool {
        false
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("columns") => Some(Value::from_object(EmptyColumns)),
            Some("rows" | "column_names" | "column_types") => Some(Value::from_object(EmptyTable)),
            _ => None,
        }
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &mut State<'_, '_>,
        method: &str,
        _args: &[Value],
    ) -> Result<Value, Error> {
        match method {
            "values" | "keys" | "items" | "print_table" => Ok(Value::from_object(EmptyTable)),
            _ => Err(Error::from(ErrorKind::UnknownMethod)),
        }
    }
}

/// An empty table's `columns`: the result's shape is unknown, so any column
/// (`table.columns[0]`) is an empty one, as in a query that returned no rows.
/// `dbt_utils.get_single_value()` then falls back to its default.
#[derive(Debug)]
struct EmptyColumns;

impl Object for EmptyColumns {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Seq
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Empty
    }

    fn is_true(self: &Arc<Self>) -> bool {
        false
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        (key.is_number() || key.as_str().is_some()).then(|| Value::from_object(EmptyTable))
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &mut State<'_, '_>,
        method: &str,
        _args: &[Value],
    ) -> Result<Value, Error> {
        match method {
            "values" | "keys" | "items" => Ok(Value::from_object(EmptyTable)),
            _ => Err(Error::from(ErrorKind::UnknownMethod)),
        }
    }
}

// ---------------------------------------------------------------------------
// Mutable list and dict

#[derive(Debug, Default)]
struct MutList(Mutex<Vec<Value>>);

impl MutList {
    fn value(items: Vec<Value>) -> Value {
        Value::from_object(MutList(Mutex::new(items)))
    }

    fn items(&self) -> Vec<Value> {
        self.0.lock().map(|v| v.clone()).unwrap_or_default()
    }
}

impl Object for MutList {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Seq
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        let items = self.0.lock().ok()?;
        let i = i64::try_from(key.clone()).ok()?;
        let i = if i < 0 { items.len() as i64 + i } else { i };
        items.get(usize::try_from(i).ok()?).cloned()
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Values(self.items())
    }

    fn enumerator_len(self: &Arc<Self>) -> Option<usize> {
        self.0.lock().ok().map(|v| v.len())
    }

    fn is_true(self: &Arc<Self>) -> bool {
        self.0.lock().is_ok_and(|v| !v.is_empty())
    }

    fn call_method(
        self: &Arc<Self>,
        state: &mut State<'_, '_>,
        method: &str,
        args: &[Value],
    ) -> Result<Value, Error> {
        let mut items = self.0.lock().map_err(|_| err("poisoned"))?;
        match method {
            "append" => {
                items.push(mutify(args.first().cloned().unwrap_or_default()));
                Ok(Value::from(()))
            }
            "extend" => {
                if let Some(more) = args.first() {
                    items.extend(more.try_iter()?.map(mutify));
                }
                Ok(Value::from(()))
            }
            "insert" => {
                let i = args
                    .first()
                    .and_then(|v| usize::try_from(v.clone()).ok())
                    .unwrap_or(0);
                let at = i.min(items.len());
                items.insert(at, args.get(1).cloned().unwrap_or_default());
                Ok(Value::from(()))
            }
            "pop" => {
                let v = match args.first().and_then(|v| usize::try_from(v.clone()).ok()) {
                    Some(i) if i < items.len() => Some(items.remove(i)),
                    _ => items.pop(),
                };
                Ok(v.unwrap_or_default())
            }
            "remove" => {
                if let Some(pos) = args.first().and_then(|a| items.iter().position(|v| v == a)) {
                    items.remove(pos);
                }
                Ok(Value::from(()))
            }
            "copy" => Ok(MutList::value(items.clone())),
            "index" => Ok(args
                .first()
                .and_then(|a| items.iter().position(|v| v == a))
                .map(Value::from)
                .unwrap_or(Value::from(-1))),
            "count" => Ok(Value::from(
                args.first()
                    .map(|a| items.iter().filter(|v| *v == a).count())
                    .unwrap_or(0),
            )),
            _ => {
                let plain = Value::from(items.clone());
                drop(items);
                minijinja_contrib::pycompat::unknown_method_callback(state, &plain, method, args)
            }
        }
    }
}

#[derive(Debug, Default)]
struct MutDict(Mutex<Vec<(Value, Value)>>);

impl MutDict {
    fn value(items: Vec<(Value, Value)>) -> Value {
        Value::from_object(MutDict(Mutex::new(items)))
    }

    fn items(&self) -> Vec<(Value, Value)> {
        self.0.lock().map(|v| v.clone()).unwrap_or_default()
    }

    fn set(items: &mut Vec<(Value, Value)>, k: Value, v: Value) {
        match items.iter_mut().find(|(key, _)| *key == k) {
            Some(slot) => slot.1 = v,
            None => items.push((k, v)),
        }
    }
}

impl Object for MutDict {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        ObjectRepr::Map
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        let items = self.0.lock().ok()?;
        items.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Values(self.items().into_iter().map(|(k, _)| k).collect())
    }

    fn enumerator_len(self: &Arc<Self>) -> Option<usize> {
        self.0.lock().ok().map(|v| v.len())
    }

    fn is_true(self: &Arc<Self>) -> bool {
        self.0.lock().is_ok_and(|v| !v.is_empty())
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &mut State<'_, '_>,
        method: &str,
        args: &[Value],
    ) -> Result<Value, Error> {
        let mut items = self.0.lock().map_err(|_| err("poisoned"))?;
        let pairs = |items: &[(Value, Value)]| -> Value {
            Value::from(
                items
                    .iter()
                    .map(|(k, v)| Value::from(vec![k.clone(), v.clone()]))
                    .collect::<Vec<_>>(),
            )
        };
        match method {
            "get" => Ok(args
                .first()
                .and_then(|k| {
                    items
                        .iter()
                        .find(|(key, _)| key == k)
                        .map(|(_, v)| v.clone())
                })
                .or_else(|| args.get(1).cloned())
                .unwrap_or_default()),
            "items" => Ok(pairs(&items)),
            "keys" => Ok(Value::from(
                items.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
            )),
            "values" => Ok(Value::from(
                items.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
            )),
            "update" => {
                for a in args {
                    if let Ok(iter) = a.try_iter() {
                        for k in iter {
                            let v = mutify(a.get_item(&k).unwrap_or_default());
                            MutDict::set(&mut items, k, v);
                        }
                    }
                }
                Ok(Value::from(()))
            }
            "pop" => {
                let k = args.first().cloned().unwrap_or_default();
                let pos = items.iter().position(|(key, _)| *key == k);
                Ok(pos
                    .map(|p| items.remove(p).1)
                    .or_else(|| args.get(1).cloned())
                    .unwrap_or_default())
            }
            "setdefault" => {
                let k = args.first().cloned().unwrap_or_default();
                if let Some((_, v)) = items.iter().find(|(key, _)| *key == k) {
                    return Ok(v.clone());
                }
                let v = args.get(1).cloned().unwrap_or_default();
                items.push((k, v.clone()));
                Ok(v)
            }
            "copy" => Ok(MutDict::value(items.clone())),
            _ => Err(Error::from(ErrorKind::UnknownMethod)),
        }
    }
}

/// A plain, `{key: value}` map or list literal as its mutable counterpart,
/// recursively.
fn mutify(v: Value) -> Value {
    if let Some(items) = v.downcast_object_ref::<Vec<Value>>() {
        return MutList::value(items.iter().cloned().map(mutify).collect());
    }
    if let Some(m) = v.downcast_object_ref::<BTreeMap<Value, Value>>() {
        return MutDict::value(
            m.iter()
                .map(|(k, v)| (k.clone(), mutify(v.clone())))
                .collect(),
        );
    }
    // A lazy sequence (`list_a + list_b`): collect it.
    if v.as_object()
        .is_some_and(|o| o.repr() == ObjectRepr::Iterable)
    {
        if let Ok(items) = v.try_iter() {
            return MutList::value(items.map(mutify).collect());
        }
    }
    v
}

type FuncImpl = dyn Fn(&mut State, &[Value]) -> Result<Value, Error> + Send + Sync;

/// A global function taking any arguments (keyword arguments included,
/// which minijinja's typed functions would reject when unused).
struct Func(Box<FuncImpl>);

impl fmt::Debug for Func {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("function")
    }
}

impl Object for Func {
    fn call(self: &Arc<Self>, state: &mut State<'_, '_>, args: &[Value]) -> Result<Value, Error> {
        (self.0)(state, args)
    }
}

fn add_fn<F>(env: &mut Environment<'static>, name: &'static str, f: F)
where
    F: Fn(&mut State, &[Value]) -> Result<Value, Error> + Send + Sync + 'static,
{
    env.add_global(name, Value::from_object(Func(Box::new(f))));
}

/// `api`: `api.Column.translate_type()`, `api.Relation.create()`.
#[derive(Debug)]
struct Api;

impl Object for Api {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "Column" | "Relation" => Some(Value::from_object(Api)),
            _ => None,
        }
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &mut State<'_, '_>,
        method: &str,
        args: &[Value],
    ) -> Result<Value, Error> {
        match method {
            "translate_type" | "string_type" | "numeric_type" => {
                Ok(arg(args, 0, "dtype").unwrap_or(Value::from("varchar")))
            }
            "create" => Ok(["identifier", "schema"]
                .iter()
                .find_map(|k| relation_node(&arg(args, 9, k)?))
                .map(relation)
                .unwrap_or_default()),
            _ => Err(Error::from(ErrorKind::UnknownMethod)),
        }
    }
}

/// Any date or time (`run_started_at`, `modules.datetime…`): a fixed
/// timestamp whose methods return itself.
#[derive(Debug)]
struct Clock;

impl Object for Clock {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str()? {
            "year" => Some(Value::from(2000)),
            "month" | "day" => Some(Value::from(1)),
            "hour" | "minute" | "second" => Some(Value::from(0)),
            _ => Some(Value::from_object(Clock)),
        }
    }

    fn call(self: &Arc<Self>, _state: &mut State<'_, '_>, _args: &[Value]) -> Result<Value, Error> {
        Ok(Value::from_object(Clock))
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &mut State<'_, '_>,
        method: &str,
        _args: &[Value],
    ) -> Result<Value, Error> {
        match method {
            "strftime" | "isoformat" | "date" => Ok(Value::from("2000-01-01")),
            _ => Ok(Value::from_object(Clock)),
        }
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("2000-01-01 00:00:00")
    }
}

// ---------------------------------------------------------------------------
// Globals

/// dbt macros the stand-ins cover, callable without the `dbt.` namespace
/// unless a project or package defines its own.
const DBT_GLOBAL_MACROS: &[&str] = &[
    "type_string",
    "type_text",
    "type_timestamp",
    "type_date",
    "type_int",
    "type_bigint",
    "type_numeric",
    "type_float",
    "type_boolean",
    "current_timestamp",
    "current_timestamp_backcompat",
    "current_timestamp_in_utc_backcompat",
    "snapshot_get_time",
    "dateadd",
    "datediff",
    "date_trunc",
    "last_day",
    "safe_cast",
    "cast_bool_to_text",
    "concat",
    "hash",
    "split_part",
];

/// Names the renderer defines itself; project macros don't shadow them.
const RESERVED: &[&str] = &[
    "ref",
    "source",
    "var",
    "env_var",
    "config",
    "is_incremental",
    "return",
    "adapter",
    "this",
    "target",
    "execute",
    "flags",
    "run_query",
    "statement",
    "load_result",
    "log",
    "print",
    "exceptions",
    "model",
    "graph",
    "api",
    "modules",
    "zip",
    "set",
    "fromjson",
    "tojson",
    "local_md5",
    "dbt_version",
    "invocation_id",
    "run_started_at",
    "builtins",
    "__list",
    "__dict",
    "__mut",
    "namespace",
    "range",
    "dict",
    "load_relation",
    "lipsum",
    "cycler",
    "joiner",
    "debug",
    "set_strict",
];

fn install_globals(env: &mut Environment<'static>, shared: &Arc<Shared>) {
    let names: Vec<String> = shared
        .all_macro_names()
        .into_iter()
        .map(str::to_string)
        .collect();
    for name in names {
        if RESERVED.contains(&name.as_str()) {
            continue;
        }
        env.add_global(
            name.clone(),
            Value::from_object(MacroRef {
                shared: SharedRef(shared.clone()),
                ns: None,
                name,
                dispatch: false,
            }),
        );
    }
    // dbt's own macros are global too (`{{ datediff(...) }}` without `dbt.`).
    let defined = shared.all_macro_names();
    for name in DBT_GLOBAL_MACROS {
        if !defined.contains(name) {
            env.add_global(
                *name,
                Value::from_object(MacroRef {
                    shared: SharedRef(shared.clone()),
                    ns: Some(DBT.to_string()),
                    name: name.to_string(),
                    dispatch: false,
                }),
            );
        }
    }
    let mut namespaces: Vec<&String> = shared.macros.keys().collect();
    namespaces.sort();
    for ns in namespaces {
        env.add_global(
            ns.clone(),
            Value::from_object(PackageNs {
                shared: SharedRef(shared.clone()),
                name: ns.clone(),
            }),
        );
    }
    if !shared.macros.contains_key(DBT) {
        env.add_global(
            DBT,
            Value::from_object(PackageNs {
                shared: SharedRef(shared.clone()),
                name: DBT.to_string(),
            }),
        );
    }

    let by_ref = shared.clone();
    add_fn(env, "ref", move |_, args| {
        let (pos, kwargs) = split_kwargs(args);
        let name = pos
            .last()
            .and_then(|v| v.as_str())
            .ok_or_else(|| err("ref() needs a name"))?
            .to_string();
        let version = kwargs.and_then(|k| {
            k.get::<Option<Value>>("v")
                .ok()
                .flatten()
                .or_else(|| k.get::<Option<Value>>("version").ok().flatten())
                .map(|v| v.to_string())
        });
        let id =
            super::ast_column_lineage::resolve_ref_in(&by_ref.by_ref, &name, version.as_deref());
        Ok(relation(id))
    });
    add_fn(env, "source", |_, args| {
        let source = arg_str(args, 0, "source_name").ok_or_else(|| err("source() name"))?;
        let table = arg_str(args, 1, "table_name").ok_or_else(|| err("source() table"))?;
        Ok(relation(format!("source.{source}.{table}")))
    });
    let vars = shared.clone();
    add_fn(env, "var", move |state, args| {
        let name = arg_str(args, 0, "name").ok_or_else(|| err("var() name"))?;
        let caller = caller_package(state);
        let model = with_state(|s| s.package.clone());
        let Some(v) = vars.var(&name, &model, &caller) else {
            return Ok(arg(args, 1, "default").unwrap_or_default());
        };
        match v.as_str() {
            Some(s) if s.contains("{{") || s.contains("{%") => {
                Ok(Value::from(state.env().render_str(s, ())?))
            }
            _ => Ok(v),
        }
    });
    add_fn(env, "env_var", |_, args| {
        Ok(arg(args, 1, "default").unwrap_or(Value::from("")))
    });
    add_fn(env, "is_incremental", |_, _| Ok(Value::from(false)));
    add_fn(env, "return", |_, args| {
        let v = mutify(args.first().cloned().unwrap_or_default());
        with_state(|s| {
            if let Some(slot) = s.returns.last_mut() {
                slot.get_or_insert(v);
            }
        });
        Ok(Value::from(""))
    });
    for name in ["log", "print", "statement", "debug"] {
        add_fn(env, name, |_, _| Ok(Value::from("")));
    }
    add_fn(env, "run_query", |_, _| Ok(Value::from_object(EmptyTable)));
    add_fn(env, "load_result", |_, _| {
        Ok(map([
            ("table", Value::from_object(EmptyTable)),
            ("data", MutList::value(vec![])),
            ("response", Value::from(())),
        ]))
    });
    add_fn(env, "load_relation", |_, args| {
        Ok(arg(args, 0, "relation")
            .and_then(|r| relation_node(&r))
            .map(relation)
            .unwrap_or_default())
    });
    add_fn(env, "__list", |_, args| {
        Ok(mutify(
            args.first()
                .cloned()
                .unwrap_or(Value::from(Vec::<Value>::new())),
        ))
    });
    add_fn(env, "__dict", |_, args| {
        Ok(mutify(args.first().cloned().unwrap_or_default()))
    });
    add_fn(env, "__mut", |_, args| {
        Ok(mutify(args.first().cloned().unwrap_or_default()))
    });
    for name in ["set", "set_strict"] {
        add_fn(env, name, |_, args| {
            let mut out: Vec<Value> = Vec::new();
            if let Some(v) = args.first() {
                for item in v.try_iter()? {
                    if !out.contains(&item) {
                        out.push(item);
                    }
                }
            }
            Ok(MutList::value(out))
        });
    }
    add_fn(env, "zip", |_, args| {
        let lists: Vec<Vec<Value>> = args
            .iter()
            .map(|a| a.try_iter().map(|i| i.collect()))
            .collect::<Result<_, _>>()?;
        let n = lists.iter().map(Vec::len).min().unwrap_or(0);
        Ok(Value::from(
            (0..n)
                .map(|i| Value::from(lists.iter().map(|l| l[i].clone()).collect::<Vec<_>>()))
                .collect::<Vec<_>>(),
        ))
    });
    add_fn(env, "local_md5", |_, args| {
        let s = args.first().map(|v| v.to_string()).unwrap_or_default();
        Ok(Value::from(
            blake3::hash(s.as_bytes()).to_hex()[..32].to_string(),
        ))
    });
    env.add_filter("as_bool", |v: Value| match v.as_str() {
        Some(s) => Value::from(s.trim().eq_ignore_ascii_case("true")),
        None => v,
    });
    env.add_filter("as_number", |v: Value| match v.as_str() {
        Some(s) => s
            .trim()
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| s.trim().parse::<f64>().map(Value::from))
            .unwrap_or(v),
        None => v,
    });
    env.add_filter("as_text", |v: Value| Value::from(v.to_string()));
    env.add_filter("as_native", |v: Value| v);
    env.add_global("api", Value::from_object(Api));
    env.add_global("run_started_at", Value::from_object(Clock));
    env.add_global(
        "adapter",
        Value::from_object(Adapter(SharedRef(shared.clone()))),
    );
    env.add_global("config", Value::from_object(Config));
    env.add_global("this", Value::from_object(This));
    env.add_global("exceptions", Value::from_object(Exceptions));
    env.add_global("modules", Value::from_object(Modules));
    env.add_global("execute", true);
    env.add_global("dbt_version", "1.9.0");
    env.add_global("invocation_id", "00000000-0000-0000-0000-000000000000");
    env.add_global(
        "target",
        map([
            ("name", Value::from("dev")),
            ("target_name", Value::from("dev")),
            ("type", Value::from("duckdb")),
            ("schema", Value::from("main")),
            ("database", Value::from("db")),
            ("profile_name", Value::from("default")),
            ("threads", Value::from(1)),
        ]),
    );
    env.add_global(
        "flags",
        map([
            ("WHICH", Value::from("compile")),
            ("FULL_REFRESH", Value::from(false)),
            ("STORE_FAILURES", Value::from(false)),
            ("WARN_ERROR", Value::from(false)),
        ]),
    );
    env.add_global(
        "graph",
        map([
            // Empty maps (an iterator of pairs would be a list).
            ("nodes", Value::from(BTreeMap::<String, Value>::new())),
            ("sources", Value::from(BTreeMap::<String, Value>::new())),
            ("macros", Value::from(BTreeMap::<String, Value>::new())),
        ]),
    );
    env.add_global(
        "model",
        map([
            ("config", Value::from_object(Config)),
            ("resource_type", Value::from("model")),
        ]),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn project(files: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (path, content) in files {
            let p = dir.path().join(path);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(p, content).unwrap();
        }
        dir
    }

    fn render(dir: &Path, sql: &str, columns: &[(&str, &[&str])]) -> Option<Rendered> {
        let by_ref = HashMap::from([
            ("orders".to_string(), "model.p.orders".to_string()),
            ("stg".to_string(), "model.p.stg".to_string()),
        ]);
        let r = Renderer::new(dir, by_ref)?;
        r.render(
            sql,
            ModelInput {
                node_id: "model.p.this_model",
                file_path: None,
                columns: columns
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.iter().map(|c| c.to_string()).collect()))
                    .collect(),
            },
        )
    }

    fn squash(s: &str) -> String {
        s.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[test]
    fn refs_and_sources_become_placeholders() {
        let dir = project(&[("dbt_project.yml", "name: p\n")]);
        let out = render(
            dir.path(),
            "{{ config(materialized='table') }}select a from {{ ref('orders') }} join {{ source('raw', 'items') }} using (a)",
            &[],
        )
        .unwrap();
        assert_eq!(
            squash(&out.sql),
            "select a from __dbtl_rel_0__ join __dbtl_rel_1__ using (a)"
        );
        assert_eq!(out.relations, vec!["model.p.orders", "source.raw.items"]);
    }

    #[test]
    fn package_macros_loops_and_return_values() {
        let dir = project(&[
            ("dbt_project.yml", "name: p\nvars:\n  prefix: c\n  utils:\n    scoped: yes_scoped\n"),
            (
                "macros/cols.sql",
                "{% macro my_cols() %}{% set cols = [] %}{% for i in range(2) %}{% do cols.append(var('prefix') ~ i) %}{% endfor %}{{ return(cols) }}{% endmacro %}",
            ),
            ("dbt_packages/utils/dbt_project.yml", "name: utils\nvars:\n  scoped: default\n"),
            (
                "dbt_packages/utils/macros/list.sql",
                "{% macro select_list(cols) %}{{ adapter.dispatch('select_list', 'utils')(cols) }}{% endmacro %}\n{% macro default__select_list(cols) %}{% for c in cols %}{{ c }} as {{ helper(c) }}{% if not loop.last %}, {% endif %}{% endfor %}{% endmacro %}\n{% macro helper(c) %}out_{{ c }}{% endmacro %}\n{% macro scoped_var() %}{{ var('scoped') }}{% endmacro %}\n{% test not_null(model) %}x{% endtest %}",
            ),
        ]);
        let out = render(
            dir.path(),
            "select {{ utils.select_list(my_cols()) }}, '{{ utils.scoped_var() }}' as v, {{ dbt.type_string() }} as t from {{ ref('stg') }}",
            &[],
        )
        .unwrap();
        assert_eq!(
            squash(&out.sql),
            "select c0 as out_c0, c1 as out_c1, 'yes_scoped' as v, varchar as t from __dbtl_rel_0__"
        );
    }

    #[test]
    fn columns_in_relation_come_from_the_caller() {
        let dir = project(&[
            ("dbt_project.yml", "name: p\n"),
            (
                "macros/star.sql",
                "{% macro star(rel) %}{% for c in adapter.get_columns_in_relation(rel) %}{{ c.name }}{% if not loop.last %}, {% endif %}{% endfor %}{% endmacro %}",
            ),
        ]);
        let out = render(
            dir.path(),
            "select {{ star(ref('stg')) }} from {{ ref('stg') }}",
            &[("model.p.stg", &["id", "name"])],
        )
        .unwrap();
        assert_eq!(squash(&out.sql), "select id, name from __dbtl_rel_1__");
        assert_eq!(out.relations, vec!["model.p.stg", "model.p.stg"]);
    }

    #[test]
    fn vars_with_jinja_render_and_defaults_apply() {
        let dir = project(&[(
            "dbt_project.yml",
            "name: p\nvars:\n  orders_table: \"{{ ref('orders') }}\"\n  enabled: false\n",
        )]);
        let out = render(
            dir.path(),
            "select {% if var('enabled') %}x{% else %}y{% endif %}, {{ var('missing', 'z') }} from {{ var('orders_table') }}{% if is_incremental() %} where 1{% endif %}",
            &[],
        )
        .unwrap();
        assert_eq!(squash(&out.sql), "select y, z from __dbtl_rel_0__");
        assert_eq!(out.relations, vec!["model.p.orders"]);
    }

    #[test]
    fn macros_read_vars_of_the_models_package() {
        let dir = project(&[
            ("dbt_project.yml", "name: p\n"),
            (
                "dbt_packages/shop/dbt_project.yml",
                "name: shop\nvars:\n  flag: true\n",
            ),
            (
                "dbt_packages/utils/dbt_project.yml",
                "name: utils\nvars:\n  flag: false\n",
            ),
            (
                "dbt_packages/utils/macros/on.sql",
                "{% macro is_on(v) %}{{ return(var(v, false)) }}{% endmacro %}",
            ),
        ]);
        let r = Renderer::new(dir.path(), HashMap::new()).unwrap();
        let out = r
            .render(
                "select {% if utils.is_on('flag') %}a{% else %}b{% endif %} from t",
                ModelInput {
                    node_id: "model.shop.m",
                    file_path: Some(Path::new("dbt_packages/shop/models/m.sql")),
                    columns: HashMap::new(),
                },
            )
            .unwrap();
        assert_eq!(squash(&out.sql), "select a from t");
        let dir = project(&[
            (
                "dbt_project.yml",
                "name: p\nvars:\n  cols: ['a', 'b']\n  shop:\n    x: 1\n",
            ),
            (
                "dbt_packages/shop/dbt_project.yml",
                "name: shop\nvars:\n  shop:\n    y: 2\n",
            ),
        ]);
        let r = Renderer::new(dir.path(), HashMap::new()).unwrap();
        let out = r
            .render(
                "select {% for c in var('cols', []) -%}{{ c }}, {% endfor -%} 1 from t",
                ModelInput {
                    node_id: "model.shop.m",
                    file_path: Some(Path::new("dbt_packages/shop/models/m.sql")),
                    columns: HashMap::new(),
                },
            )
            .unwrap();
        assert_eq!(squash(&out.sql), "select a, b, 1 from t");
    }

    #[test]
    fn dbt_macros_are_global_and_graph_maps_are_empty() {
        let dir = project(&[("dbt_project.yml", "name: p\n")]);
        let out = render(
            dir.path(),
            "select {{ datediff('a', 'b', 'day') }} as d, {{ type_string() }} as t, {{ graph.nodes.values() | list | length }} as n from t",
            &[],
        )
        .unwrap();
        assert_eq!(
            squash(&out.sql),
            "select date_diff('day', a, b) as d, varchar as t, 0 as n from t"
        );
    }

    #[test]
    fn query_results_are_empty() {
        let dir = project(&[
            ("dbt_project.yml", "name: p\n"),
            (
                "macros/single.sql",
                "{% macro single(query, default=none) %}{%- call statement('q', fetch_result=True) -%}{{ query }}{%- endcall -%}{% set r = load_result('q').table.columns[0].values() %}{% if r | length == 0 %}{% set v = default %}{% else %}{% set v = r[0] %}{% endif %}{% do return(v) %}{% endmacro %}",
            ),
        ]);
        let out = render(
            dir.path(),
            "select cast('{{ single('select max(d) from x', '2020-01-01') }}' as date) as d, {{ run_query('select 1').columns['a'].values() | length }} as n from t",
            &[],
        )
        .unwrap();
        assert_eq!(
            squash(&out.sql),
            "select cast('2020-01-01' as date) as d, 0 as n from t"
        );
    }

    #[test]
    fn defaults_read_earlier_parameters() {
        let dir = project(&[
            ("dbt_project.yml", "name: p\n"),
            (
                "macros/m.sql",
                "{% macro todict(l) %}{%- set d = {} -%}{%- for c in l -%}{%- do d.update({c.name: c}) if not c.is_rename -%}{%- endfor -%}{{ return(d) }}{% endmacro %}\n{% macro cr(k, d, orig=d[k]['name'], alias=d[k]['alias'] | default(orig)) %}{{ orig }} as {{ alias }}{% endmacro %}",
            ),
        ]);
        let out = render(
            dir.path(),
            "{% set l = [{'name': 'id'},{'name': 'Id', 'is_rename': true}] %}{% set d = todict(l) %}select {{ cr('id', d) }}, {{ cr('id', d, alias='x') }}, {{ cr('id', d, orig='y') }} from t",
            &[],
        )
        .unwrap();
        assert_eq!(squash(&out.sql), "select id as id, id as x, y as y from t");
        assert_eq!(
            late_defaults("{%- macro m(a, b='a', c=a ~ b, d=[1, 2]) -%}x"),
            "{%- macro m(a, b='a',c='__dbtl_unset__', d=[1, 2]) -%}{% if c == '__dbtl_unset__' %}{% set c = a ~ b %}{% endif -%}x"
        );
    }

    #[test]
    fn unsupported_jinja_fails() {
        let dir = project(&[("dbt_project.yml", "name: p\n")]);
        assert!(render(dir.path(), "select {{ undefined_macro() }} from t", &[]).is_none());
        assert!(render(
            dir.path(),
            "select {{ exceptions.raise_compiler_error('no') }}",
            &[]
        )
        .is_none());
        assert!(render(dir.path(), "select {% for %}", &[]).is_none());
    }

    #[test]
    fn set_literals_become_mutable() {
        assert_eq!(
            wrap_set_literals("{% set a = [1, ']'] %}{% set b = {'k': [1]} -%}{% set c = x[0] %}"),
            "{% set a = __list([1, ']']) %}{% set b = __dict({'k': [1]}) -%}{% set c = __mut(x[0]) %}"
        );
        assert_eq!(
            wrap_set_literals("{%- set c = (['a'] + var('x', [])) if y else '%}' -%}"),
            "{%- set c = __mut((['a'] + var('x', [])) if y else '%}') -%}"
        );
        let dir = project(&[("dbt_project.yml", "name: p\n")]);
        let out = render(
            dir.path(),
            "{% set d = {} %}{% do d.update({'a': 1}) %}{% set l = [] %}{% do l.append('b') %}{% do l.extend(['c']) %}{% set m = (['x'] + var('none', [])) %}{% do m.append('y') %}{% set n = l %}{% do n.append('d') %}select {{ d['a'] }}, {{ l | join(', ') }}, {{ m | join(', ') }}",
            &[],
        )
        .unwrap();
        assert_eq!(squash(&out.sql), "select 1, b, c, d, x, y");
    }

    #[test]
    fn package_of_finds_the_package_directory() {
        let dirs = HashMap::from([("hub".to_string(), "hubspot".to_string())]);
        assert_eq!(
            package_of(Path::new("dbt_packages/hub/models/x.sql"), &dirs).as_deref(),
            Some("hubspot")
        );
        assert_eq!(package_of(Path::new("models/x.sql"), &dirs), None);
    }
}
