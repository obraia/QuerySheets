mod common;

use common::{create_customers_fixture, run_cli_export};
use std::error::Error;
use std::fs;
use tempfile::tempdir;

#[test]
fn query_exports_csv_with_header_and_rows() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    let output = tmp.path().join("customers_export.csv");
    create_customers_fixture(&fixture)?;

    let sql = "SELECT CustomerId, CustomerName, AccountStatus FROM Customers WHERE Segment = 'Enterprise'";
    run_cli_export(&fixture, sql, None, &output)?;

    let content = fs::read_to_string(&output)?;
    let lines = content.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "CustomerId,CustomerName,AccountStatus");
    assert_eq!(lines[1], "C-001,Alice Johnson,Active");
    assert_eq!(lines[2], "C-003,Carla Davis,Churned");

    Ok(())
}

#[test]
fn query_exports_json_as_object_array() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    let output = tmp.path().join("customers_export.json");
    create_customers_fixture(&fixture)?;

    let sql = "SELECT CustomerId AS ClientId, 1 + 2 AS PriorityScore FROM Customers WHERE CustomerId = 'C-001'";
    run_cli_export(&fixture, sql, None, &output)?;

    let content = fs::read_to_string(&output)?;
    let data: serde_json::Value = serde_json::from_str(&content)?;
    let rows = data.as_array().ok_or("expected top-level JSON array")?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["ClientId"], "C-001");
    assert_eq!(rows[0]["PriorityScore"], 3);

    Ok(())
}
