use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::Context;
use duckdb::Connection;

use crate::utils::{MEMORY_DB, escape_identifier, escape_literal};

/// Keeps the temporary DuckLake directory alive for as long as it is held.
///
/// Dropping the last owner removes the session database file and data files.
#[must_use = "dropping this deletes the DuckLake data directory"]
pub struct DataDir {
    path: PathBuf,
}

impl Drop for DataDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// A lakehouse connection plus the temp directory its files live in.
pub struct SharedLakehouse {
    pub connection: std::sync::Arc<Mutex<Connection>>,
    _data_dir: DataDir,
}

impl SharedLakehouse {
    pub fn open() -> anyhow::Result<Self> {
        let (connection, data_dir) = open()?;
        Ok(Self {
            connection: std::sync::Arc::new(Mutex::new(connection)),
            _data_dir: data_dir,
        })
    }
}

/// Opens a DuckDB session and attaches a DuckLake named `memory`.
///
/// The catalog metadata is an in-memory DuckDB database (`ducklake::memory:`).
/// Table data is written under a temporary directory. The session database itself
/// is a normal file in that directory: a path such as `:memory:session` is not
/// an in-memory database and DuckDB creates a file with that name.
pub fn open() -> anyhow::Result<(Connection, DataDir)> {
    let root = std::env::temp_dir().join(format!("altertable-mock-{}", uuid::Uuid::now_v7()));
    let data_path = root.join("data");
    std::fs::create_dir_all(&data_path).with_context(|| {
        format!(
            "failed to create DuckLake data directory {}",
            data_path.display()
        )
    })?;

    let shell_path = root.join("session.duckdb");
    let connection = Connection::open(&shell_path).with_context(|| {
        format!(
            "failed to open DuckDB session database {}",
            shell_path.display()
        )
    })?;

    if let Err(err) = attach(&connection, &data_path) {
        drop(connection);
        let _ = std::fs::remove_dir_all(&root);
        return Err(err);
    }

    Ok((connection, DataDir { path: root }))
}

fn attach(connection: &Connection, data_path: &Path) -> anyhow::Result<()> {
    load_ducklake(connection)?;

    let catalog = escape_identifier(MEMORY_DB);
    let data_path = escape_literal(&data_path.to_string_lossy());
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake::memory:' AS \"{catalog}\" (DATA_PATH '{data_path}', DATA_INLINING_ROW_LIMIT 0);
             USE \"{catalog}\";"
        ))
        .context("failed to attach in-memory DuckLake")?;
    Ok(())
}

fn load_ducklake(connection: &Connection) -> anyhow::Result<()> {
    connection
        .execute_batch("LOAD ducklake;")
        .context("failed to load the DuckLake extension (run `INSTALL ducklake;` first)")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_files_in_the_temp_directory() {
        let cwd = std::env::current_dir().unwrap();
        let (connection, data_dir) = open().unwrap();
        connection
            .execute_batch("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1);")
            .unwrap();

        assert!(data_dir.path.starts_with(std::env::temp_dir()));
        assert!(data_dir.path.join("session.duckdb").is_file());
        assert!(data_dir.path.join("data").is_dir());
        for name in [":memory:session", ":memory:engine", ":memory:"] {
            assert!(
                !cwd.join(name).exists(),
                "{name} was created in {}",
                cwd.display()
            );
        }
    }

    #[test]
    fn cloned_connection_sees_ducklake_rows() {
        let (connection, _data_dir) = open().unwrap();
        connection
            .execute_batch("CREATE TABLE shared (id INTEGER); INSERT INTO shared VALUES (7);")
            .unwrap();

        let clone = connection.try_clone().unwrap();
        clone.execute_batch("USE memory;").unwrap();
        let id: i32 = clone
            .query_row("SELECT id FROM shared", [], |row| row.get(0))
            .unwrap();
        assert_eq!(id, 7);
    }

    #[test]
    fn rejects_primary_key_and_unique() {
        let (connection, _data_dir) = open().unwrap();
        let primary_key = connection
            .execute_batch("CREATE TABLE pk_t (id INTEGER PRIMARY KEY);")
            .unwrap_err()
            .to_string();
        assert!(
            primary_key.contains("not supported in DuckLake"),
            "{primary_key}"
        );

        let unique = connection
            .execute_batch("CREATE TABLE uniq_t (name VARCHAR UNIQUE);")
            .unwrap_err()
            .to_string();
        assert!(unique.contains("not supported in DuckLake"), "{unique}");
    }
}
