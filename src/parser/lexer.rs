//! Lightweight SQL+Jinja lexer (#25). Produces a "cleaned" view of source
//! where SQL string literals and SQL/Jinja comments are blanked out but
//! Jinja expressions (`{{ ... }}`) and statements (`{% ... %}`) are kept
//! intact. Existing regex-based ref/source extractors then run over the
//! cleaned text without tripping over `'I prefer X'` strings or `-- ref(`
//! comments.
//!
//! Trade-off: this is a hand-rolled state machine, not a full Jinja parser.
//! It handles the structural questions ("is this position inside a string,
//! comment, or code?") and intentionally stays dumb about everything else.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Code,
    SqlString { quote: u8 },
    SqlLineComment,
    SqlBlockComment,
    JinjaExpr,
    JinjaStmt,
    JinjaComment,
}

/// Blank out string literals and comments while keeping Jinja blocks intact.
/// The output has the same length and the same line breaks as the input — so
/// regex match positions (if a caller needs them) still map back to the
/// original source line/column.
pub fn strip_non_code(src: &str) -> String {
    let bytes = src.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut state = State::Code;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match state {
            State::Code => {
                // Open Jinja regions first — they're two-char tokens.
                if i + 1 < bytes.len() && b == b'{' {
                    match bytes[i + 1] {
                        b'{' => {
                            // {{ ... }} — Jinja expression. Pass through.
                            out.push(b);
                            out.push(bytes[i + 1]);
                            state = State::JinjaExpr;
                            i += 2;
                            continue;
                        }
                        b'%' => {
                            out.push(b);
                            out.push(bytes[i + 1]);
                            state = State::JinjaStmt;
                            i += 2;
                            continue;
                        }
                        b'#' => {
                            // {# ... #} — Jinja comment. Blank out.
                            out.push(b' ');
                            out.push(b' ');
                            state = State::JinjaComment;
                            i += 2;
                            continue;
                        }
                        _ => {}
                    }
                }
                if i + 1 < bytes.len() && b == b'-' && bytes[i + 1] == b'-' {
                    out.push(b' ');
                    out.push(b' ');
                    state = State::SqlLineComment;
                    i += 2;
                    continue;
                }
                if i + 1 < bytes.len() && b == b'/' && bytes[i + 1] == b'*' {
                    out.push(b' ');
                    out.push(b' ');
                    state = State::SqlBlockComment;
                    i += 2;
                    continue;
                }
                if b == b'\'' || b == b'"' {
                    out.push(b' ');
                    state = State::SqlString { quote: b };
                    i += 1;
                    continue;
                }
                out.push(b);
                i += 1;
            }
            State::SqlString { quote } => {
                // Doubled-quote escape ('' or "") stays inside the string.
                if b == quote {
                    if i + 1 < bytes.len() && bytes[i + 1] == quote {
                        out.push(b' ');
                        out.push(b' ');
                        i += 2;
                        continue;
                    }
                    out.push(b' ');
                    state = State::Code;
                    i += 1;
                    continue;
                }
                // Backslash escape: skip the next byte to handle '\''.
                if b == b'\\' && i + 1 < bytes.len() {
                    out.push(b' ');
                    out.push(b' ');
                    i += 2;
                    continue;
                }
                // Preserve newlines so positions line up.
                out.push(if b == b'\n' { b'\n' } else { b' ' });
                i += 1;
            }
            State::SqlLineComment => {
                if b == b'\n' {
                    out.push(b'\n');
                    state = State::Code;
                } else {
                    out.push(b' ');
                }
                i += 1;
            }
            State::SqlBlockComment => {
                if i + 1 < bytes.len() && b == b'*' && bytes[i + 1] == b'/' {
                    out.push(b' ');
                    out.push(b' ');
                    state = State::Code;
                    i += 2;
                    continue;
                }
                out.push(if b == b'\n' { b'\n' } else { b' ' });
                i += 1;
            }
            State::JinjaComment => {
                if i + 1 < bytes.len() && b == b'#' && bytes[i + 1] == b'}' {
                    out.push(b' ');
                    out.push(b' ');
                    state = State::Code;
                    i += 2;
                    continue;
                }
                out.push(if b == b'\n' { b'\n' } else { b' ' });
                i += 1;
            }
            State::JinjaExpr => {
                // }} closes the expression. Pass everything through unchanged
                // so ref('name') keeps its quoted argument intact.
                if i + 1 < bytes.len() && b == b'}' && bytes[i + 1] == b'}' {
                    out.push(b);
                    out.push(bytes[i + 1]);
                    state = State::Code;
                    i += 2;
                    continue;
                }
                out.push(b);
                i += 1;
            }
            State::JinjaStmt => {
                if i + 1 < bytes.len() && b == b'%' && bytes[i + 1] == b'}' {
                    out.push(b);
                    out.push(bytes[i + 1]);
                    state = State::Code;
                    i += 2;
                    continue;
                }
                out.push(b);
                i += 1;
            }
        }
    }
    // Should always succeed — we only push valid ASCII or pass-through UTF-8.
    String::from_utf8(out).unwrap_or_else(|_| src.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_passes_plain_sql_unchanged() {
        let sql = "select id from users";
        assert_eq!(strip_non_code(sql), sql);
    }

    #[test]
    fn test_blanks_line_comment() {
        let sql = "select id -- this is {{ ref('hidden') }}\nfrom users";
        let cleaned = strip_non_code(sql);
        assert!(!cleaned.contains("ref('hidden')"));
        // Length and line break preserved.
        assert_eq!(cleaned.len(), sql.len());
        assert!(cleaned.contains('\n'));
    }

    #[test]
    fn test_blanks_block_comment() {
        let sql = "select /* {{ ref('bad') }} */ id from users";
        let cleaned = strip_non_code(sql);
        assert!(!cleaned.contains("ref('bad')"));
    }

    #[test]
    fn test_blanks_string_literal() {
        let sql = "select 'this looks like {{ ref(x) }} but isnt' from users";
        let cleaned = strip_non_code(sql);
        assert!(!cleaned.contains("ref(x)"));
    }

    #[test]
    fn test_preserves_jinja_expression() {
        let sql = "select * from {{ ref('orders') }}";
        let cleaned = strip_non_code(sql);
        assert!(cleaned.contains("{{ ref('orders') }}"));
    }

    #[test]
    fn test_preserves_jinja_statement() {
        let sql = "{% set x = 1 %}select {{ x }}";
        let cleaned = strip_non_code(sql);
        assert!(cleaned.contains("{% set x = 1 %}"));
    }

    #[test]
    fn test_strips_jinja_comment_inside() {
        let sql = "select id {# {{ ref('hidden') }} #} from users";
        let cleaned = strip_non_code(sql);
        assert!(!cleaned.contains("ref('hidden')"));
    }

    #[test]
    fn test_doubled_quote_escape_in_string() {
        // SQL standard: '' inside a single-quoted string is an escaped quote.
        let sql = "select 'it''s {{ ref(bad) }}' from t";
        let cleaned = strip_non_code(sql);
        assert!(!cleaned.contains("ref(bad)"));
    }

    #[test]
    fn test_double_dash_inside_string_not_a_comment() {
        let sql = "select 'value -- {{ ref(bad) }}' from t";
        let cleaned = strip_non_code(sql);
        assert!(!cleaned.contains("ref(bad)"));
    }

    #[test]
    fn test_preserves_byte_length_and_newlines() {
        let sql = "select 1 -- comment\nfrom t /* multi\nline */ where id = 'x'\n";
        let cleaned = strip_non_code(sql);
        assert_eq!(cleaned.len(), sql.len());
        // Same number of \n characters so line numbers stay aligned.
        let nl_in = sql.bytes().filter(|&b| b == b'\n').count();
        let nl_out = cleaned.bytes().filter(|&b| b == b'\n').count();
        assert_eq!(nl_in, nl_out);
    }

    #[test]
    fn test_multibyte_utf8_inside_comment_is_safe() {
        // Mirrors GH issue #1's reproducer — multi-byte chars inside a
        // line comment must not cause panics or trip char boundaries.
        let sql = "select case when flag = true then false -- 日本語コメント\n  else flag end as flag from t";
        let cleaned = strip_non_code(sql);
        assert!(cleaned.contains("as flag"));
    }
}
