use anyhow::Context;
use duckdb::Connection;

pub fn load_ducklake(connection: &Connection) -> anyhow::Result<()> {
    install_and_load(connection, "ducklake", "INSTALL ducklake")
}

pub fn load_semantic_layer(connection: &Connection) -> anyhow::Result<()> {
    install_and_load(
        connection,
        "semantic_layer",
        "INSTALL semantic_layer FROM community",
    )
}

/// Loads an extension, installing it first when it is not already present.
///
/// A pre-downloaded extension makes `LOAD` succeed, so `INSTALL` does not run.
fn install_and_load(connection: &Connection, extension: &str, install: &str) -> anyhow::Result<()> {
    if connection
        .execute_batch(&format!("LOAD {extension};"))
        .is_ok()
    {
        return Ok(());
    }
    connection
        .execute_batch(&format!("{install}; LOAD {extension};"))
        .with_context(|| format!("failed to install and load the {extension} extension"))
}

#[cfg(test)]
mod tests {
    use duckdb::Connection;

    use super::*;

    #[test]
    fn semantic_view_sums_revenue_by_region() {
        let connection = Connection::open_in_memory().unwrap();
        load_semantic_layer(&connection).unwrap();

        let loaded: bool = connection
            .query_row(
                "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'semantic_layer'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(loaded);

        connection
            .execute_batch(
                "CREATE TABLE orders (region VARCHAR, amount INTEGER);
                 INSERT INTO orders VALUES ('US', 100), ('US', 50), ('EU', 40);
                 CREATE SEMANTIC VIEW sales
                   TABLES (
                     orders AS orders
                   )
                   DIMENSIONS (
                     orders.region AS region
                   )
                   METRICS (
                     orders.revenue AS SUM(orders.amount)
                   );",
            )
            .unwrap();

        let mut statement = connection
            .prepare(
                "SELECT region, CAST(revenue AS BIGINT)
                 FROM SEMANTIC_VIEW(
                   sales
                   DIMENSIONS orders.region
                   METRICS orders.revenue
                 )
                 ORDER BY region",
            )
            .unwrap();
        let rows: Vec<(String, i64)> = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(rows, vec![("EU".to_string(), 40), ("US".to_string(), 150)]);
    }
}
