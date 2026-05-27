use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Represents a node in the DuckDB EXPLAIN plan tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainPlan {
    pub name: String,
    #[serde(default)]
    pub children: Vec<Self>,
    #[serde(default)]
    pub extra_info: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct TableScan {
    pub table_name: String,
    pub estimated_rows: u64,
    pub filters: Option<String>,
}

impl ExplainPlan {
    pub fn table_scans(&self) -> Vec<TableScan> {
        self.find_table_scans()
            .into_iter()
            .filter_map(|scan| {
                let table_name = scan.table_name()?;
                Some(TableScan {
                    table_name: table_name.to_owned(),
                    estimated_rows: scan.estimated_cardinality().unwrap_or(0),
                    filters: scan.filters(),
                })
            })
            .collect()
    }

    fn operator_name(&self) -> &str {
        self.name.trim()
    }

    fn is_table_scan(&self) -> bool {
        matches!(self.operator_name(), "DUCKLAKE_SCAN" | "SEQ_SCAN")
    }

    fn table_name(&self) -> Option<&str> {
        self.extra_info.get("Table").and_then(|v| v.as_str())
    }

    fn estimated_cardinality(&self) -> Option<u64> {
        self.extra_info
            .get("Estimated Cardinality")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
    }

    fn filters(&self) -> Option<String> {
        match self.extra_info.get("Filters") {
            Some(serde_json::Value::String(s)) => Some(s.trim().to_owned()),
            Some(serde_json::Value::Array(arr)) => {
                let filters: Vec<String> = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.trim().to_owned()))
                    .collect();
                if filters.is_empty() {
                    None
                } else {
                    Some(filters.join(" AND "))
                }
            }
            _ => None,
        }
    }

    fn find_table_scans(&self) -> Vec<&Self> {
        let mut scans = Vec::new();
        self.collect_table_scans(&mut scans);
        scans
    }

    fn collect_table_scans<'a>(&'a self, scans: &mut Vec<&'a Self>) {
        if self.is_table_scan() {
            scans.push(self);
        }
        for child in &self.children {
            child.collect_table_scans(scans);
        }
    }
}

pub fn explain_statement(conn: &duckdb::Connection, sql: &str) -> anyhow::Result<ExplainPlan> {
    let explain_sql = format!("EXPLAIN (FORMAT JSON) {sql}");
    let mut stmt = conn
        .prepare(&explain_sql)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut rows = stmt.query([])?;
    let json_str = match rows.next()? {
        Some(row) => row.get::<_, String>(1)?,
        None => anyhow::bail!("EXPLAIN returned no rows"),
    };

    let plans: Vec<ExplainPlan> = serde_json::from_str(&json_str)
        .map_err(|e| anyhow::anyhow!("failed to parse EXPLAIN JSON: {e}"))?;

    plans
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("EXPLAIN returned empty plan array"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_explain() {
        let json = r#"{
            "name": "DUCKLAKE_SCAN ",
            "children": [],
            "extra_info": {
                "Table": "test_table",
                "Projections": ["id", "name"],
                "Filters": "id>2",
                "Estimated Cardinality": "1"
            }
        }"#;

        let plan: ExplainPlan = serde_json::from_str(json).unwrap();
        let scans = plan.table_scans();

        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].table_name, "test_table");
        assert_eq!(scans[0].estimated_rows, 1);
        assert_eq!(scans[0].filters, Some("id>2".to_owned()));
    }

    #[test]
    fn test_explain_statement_against_duckdb() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE events (id INTEGER, category VARCHAR)", [])
            .unwrap();
        conn.execute(
            "INSERT INTO events SELECT i, CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END FROM generate_series(1, 100) t(i)",
            [],
        )
        .unwrap();

        let plan = explain_statement(&conn, "SELECT * FROM events WHERE id > 50").unwrap();
        let scans = plan.table_scans();

        assert_eq!(scans.len(), 1);
        assert!(scans[0].table_name.ends_with("events"));
        assert_eq!(scans[0].filters.as_deref(), Some("id>50"));
    }
}
