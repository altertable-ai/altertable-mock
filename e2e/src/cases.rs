//! Golden query cases: SQL sent to `POST /query` and the NDJSON it must answer.
//!
//! A case file is a sequence of blocks separated by blank lines. `#` lines are comments.
//!
//! ```text
//! # Runs the SQL and only checks that it did not fail.
//! statement ok
//! CREATE TABLE t (id INTEGER)
//!
//! # Compares every response line after the header (columns, then rows, or the error).
//! query
//! SELECT 42 AS answer
//! ----
//! [{"name":"answer","type":"INTEGER"}]
//! [42]
//!
//! # A JSON object after the directive is merged into the request body.
//! query {"params": {"value": 1}}
//! SELECT $value AS value
//! ----
//! ...
//! ```
//!
//! The header line carries a fresh query ID and timing, so it is only checked for the echoed
//! statement. Lines are compared as JSON values. Set `E2E_BLESS=1` to rewrite the expected
//! output with what the mock returns.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use crate::LakehouseClient;

pub const BLESS_ENV: &str = "E2E_BLESS";
const SEPARATOR: &str = "----";

pub struct CaseFile {
    pub path: PathBuf,
    items: Vec<Item>,
}

enum Item {
    /// Comments and blank lines, kept so blessing preserves them.
    Text(String),
    Case(Case),
}

struct Case {
    line: usize,
    directive: Directive,
    directive_line: String,
    options: Map<String, Value>,
    sql: Vec<String>,
    expected: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Directive {
    Statement,
    Query,
}

impl CaseFile {
    pub fn load(path: &Path) -> Self {
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
        let items = parse(&text).unwrap_or_else(|e| panic!("{}:{e}", path.display()));
        Self {
            path: path.to_owned(),
            items,
        }
    }

    /// The file stem, which also names the user the file runs as.
    pub fn name(&self) -> &str {
        self.path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .expect("case file names are UTF-8")
    }

    /// Runs every case in order and returns one message per failure.
    ///
    /// When blessing, query cases take the actual output instead of failing on it.
    pub fn run(&mut self, client: &LakehouseClient, bless: bool) -> Vec<String> {
        let mut failures = Vec::new();
        for item in &mut self.items {
            let Item::Case(case) = item else { continue };
            if let Err(message) = case.run(client, bless) {
                failures.push(format!(
                    "{}:{}\n{}\n{message}",
                    self.path.display(),
                    case.line,
                    case.sql.join("\n")
                ));
            }
        }
        failures
    }

    pub fn write(&self) {
        let mut text = String::new();
        for item in &self.items {
            match item {
                Item::Text(line) => {
                    text.push_str(line);
                    text.push('\n');
                }
                Item::Case(case) => {
                    text.push_str(&case.directive_line);
                    text.push('\n');
                    for line in case.sql.iter().chain(&case.expected_section()) {
                        text.push_str(line);
                        text.push('\n');
                    }
                }
            }
        }
        std::fs::write(&self.path, text)
            .unwrap_or_else(|e| panic!("cannot write {}: {e}", self.path.display()));
    }
}

/// Loads every `*.case` file in a directory, sorted by name.
pub fn load_dir(dir: &Path) -> Vec<CaseFile> {
    let mut paths = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot list {}: {e}", dir.display()))
        .map(|entry| entry.expect("readable directory entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "case"))
        .collect::<Vec<_>>();
    paths.sort();
    paths.iter().map(|path| CaseFile::load(path)).collect()
}

pub fn bless_requested() -> bool {
    std::env::var(BLESS_ENV).is_ok_and(|value| !value.is_empty() && value != "0")
}

impl Case {
    fn expected_section(&self) -> Vec<String> {
        if self.directive == Directive::Statement {
            return Vec::new();
        }
        std::iter::once(SEPARATOR.to_owned())
            .chain(self.expected.iter().cloned())
            .collect()
    }

    fn run(&mut self, client: &LakehouseClient, bless: bool) -> Result<(), String> {
        let statement = self.sql.join("\n");
        let mut request = self.options.clone();
        request.insert("statement".to_owned(), Value::String(statement.clone()));

        let response = client
            .query(&Value::Object(request))
            .map_err(|e| format!("request failed: {e}"))?;
        if response.status != 200 {
            return Err(format!(
                "expected HTTP 200, got {}: {}",
                response.status, response.body
            ));
        }

        let mut lines = response.body.lines();
        let header: Value = lines
            .next()
            .ok_or("the response is empty")
            .and_then(|line| serde_json::from_str(line).map_err(|_| "the header is not JSON"))?;
        if header["statement"] != statement.as_str() {
            return Err(format!("the header echoes another statement: {header}"));
        }
        let actual = lines.map(str::to_owned).collect::<Vec<_>>();

        match self.directive {
            Directive::Statement => match actual.first() {
                Some(line) if is_error(line) => Err(format!("the statement failed: {line}")),
                _ => Ok(()),
            },
            Directive::Query if bless => {
                self.expected = actual;
                Ok(())
            }
            Directive::Query => compare(&self.expected, &actual),
        }
    }
}

fn is_error(line: &str) -> bool {
    serde_json::from_str::<Value>(line).is_ok_and(|value| value.get("error").is_some())
}

fn compare(expected: &[String], actual: &[String]) -> Result<(), String> {
    let parse = |line: &String| serde_json::from_str::<Value>(line).ok();
    let matches = expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(e, a)| parse(e).is_some_and(|e| Some(e) == parse(a)));
    if matches {
        return Ok(());
    }

    let mut message = String::from("expected:\n");
    for line in expected {
        let _ = writeln!(message, "  {line}");
    }
    message.push_str("actual:\n");
    for line in actual {
        let _ = writeln!(message, "  {line}");
    }
    Err(message)
}

fn parse(text: &str) -> Result<Vec<Item>, String> {
    let mut items = Vec::new();
    let mut lines = text.lines().enumerate().peekable();

    while let Some((index, line)) = lines.next() {
        let line_number = index + 1;
        let (directive, rest) = if let Some(rest) = line.strip_prefix("statement ok") {
            (Directive::Statement, rest)
        } else if let Some(rest) = line.strip_prefix("query") {
            (Directive::Query, rest)
        } else if line.trim().is_empty() || line.starts_with('#') {
            items.push(Item::Text(line.to_owned()));
            continue;
        } else {
            return Err(format!(
                "{line_number}: expected `statement ok`, `query`, a comment or a blank line"
            ));
        };

        let options = match rest.trim() {
            "" => Map::new(),
            json => match serde_json::from_str(json) {
                Ok(Value::Object(options)) => options,
                _ => return Err(format!("{line_number}: options must be a JSON object")),
            },
        };

        let mut sql = Vec::new();
        let mut has_separator = false;
        while let Some((_, line)) = lines.next_if(|(_, line)| !line.trim().is_empty()) {
            if line == SEPARATOR {
                has_separator = true;
                break;
            }
            sql.push(line.to_owned());
        }
        if sql.is_empty() {
            return Err(format!("{line_number}: the case has no SQL"));
        }

        let mut expected = Vec::new();
        if has_separator {
            while let Some((_, line)) = lines.next_if(|(_, line)| !line.trim().is_empty()) {
                expected.push(line.to_owned());
            }
        }
        match directive {
            Directive::Statement if has_separator => {
                return Err(format!(
                    "{line_number}: `statement ok` has no expected output"
                ));
            }
            Directive::Query if !has_separator => {
                return Err(format!("{line_number}: `query` needs a `{SEPARATOR}` line"));
            }
            _ => {}
        }

        items.push(Item::Case(Case {
            line: line_number,
            directive,
            directive_line: line.to_owned(),
            options,
            sql,
            expected,
        }));
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_reprints_unchanged() {
        let text = "# setup\nstatement ok\nCREATE TABLE t (id INTEGER)\n\nquery {\"limit\": 1}\nSELECT *\nFROM t\n----\n[{\"name\":\"id\",\"type\":\"INTEGER\"}]\n";
        let items = parse(text).unwrap();
        assert_eq!(items.len(), 4);

        let dir = std::env::temp_dir().join(format!("e2e-case-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = CaseFile {
            path: dir.join("roundtrip.case"),
            items,
        };
        file.write();
        assert_eq!(std::fs::read_to_string(&file.path).unwrap(), text);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_a_query_without_expected_output() {
        assert!(parse("query\nSELECT 1\n").is_err());
    }

    #[test]
    fn compares_lines_as_json() {
        let expected = vec![r#"{"name": "a", "type": "INTEGER"}"#.to_owned()];
        let actual = vec![r#"{"type":"INTEGER","name":"a"}"#.to_owned()];
        assert!(compare(&expected, &actual).is_ok());
        assert!(compare(&expected, &[]).is_err());
    }
}
