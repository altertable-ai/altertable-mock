use std::path::Path;

use altertable_mock_e2e::{LakehouseClient, MockContainer, PASSWORD, cases};

/// Runs every `cases/lakehouse/*.case` file, each as its own user so files never share data.
#[test]
fn lakehouse_query_cases() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("cases/lakehouse");
    let mut files = cases::load_dir(&dir);
    assert!(!files.is_empty(), "no case files in {}", dir.display());

    let users = files.iter().map(cases::CaseFile::name).collect::<Vec<_>>();
    let mock = MockContainer::start(&users);

    let bless = cases::bless_requested();
    let mut failures = Vec::new();
    for file in &mut files {
        failures.extend(file.run(&mock.lakehouse(file.name()), bless));
        if bless {
            file.write();
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) failed (rerun with {}=1 to accept the actual output):\n\n{}",
        failures.len(),
        cases::BLESS_ENV,
        failures.join("\n\n")
    );
}

#[test]
fn lakehouse_rejects_bad_credentials() {
    let mock = MockContainer::start(&["alice"]);
    let request = serde_json::json!({ "statement": "SELECT 1" });

    let wrong_password = LakehouseClient::new(&mock.lakehouse_url, "alice", "wrong");
    assert_eq!(wrong_password.query(&request).unwrap().status, 401);

    let unknown_user = LakehouseClient::new(&mock.lakehouse_url, "mallory", PASSWORD);
    assert_eq!(unknown_user.query(&request).unwrap().status, 401);
}
