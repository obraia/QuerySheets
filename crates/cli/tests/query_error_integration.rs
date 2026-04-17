mod common;

use assert_cmd::Command;
use common::{
    create_customers_fixture, create_customers_fixture_with_rows, create_customers_orders_fixture,
};
use std::error::Error;
use tempfile::tempdir;

#[test]
fn query_returns_error_for_invalid_statement() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let mut command = Command::cargo_bin("query-sheets")?;
    let assert = command
        .arg("query")
        .arg("--file")
        .arg(&fixture)
        .arg("--sql")
        .arg("DELETE FROM Customers")
        .assert()
        .failure();

    let stderr = String::from_utf8(assert.get_output().stderr.clone())?;
    assert!(stderr.contains("only SELECT statements are supported"));

    Ok(())
}

#[test]
fn query_returns_error_for_unknown_sheet_flag() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let mut command = Command::cargo_bin("query-sheets")?;
    let assert = command
        .arg("query")
        .arg("--file")
        .arg(&fixture)
        .arg("--sheet")
        .arg("MissingSheet")
        .arg("--sql")
        .arg("SELECT CustomerId FROM Customers")
        .assert()
        .failure();

    let stderr = String::from_utf8(assert.get_output().stderr.clone())?;
    assert!(stderr.contains("worksheet 'MissingSheet'"));

    Ok(())
}

#[test]
fn query_returns_error_for_unsupported_output_extension() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    let output = tmp.path().join("output.txt");
    create_customers_fixture(&fixture)?;

    let mut command = Command::cargo_bin("query-sheets")?;
    let assert = command
        .arg("query")
        .arg("--file")
        .arg(&fixture)
        .arg("--sql")
        .arg("SELECT CustomerId FROM Customers")
        .arg("--output")
        .arg(&output)
        .assert()
        .failure();

    let stderr = String::from_utf8(assert.get_output().stderr.clone())?;
    assert!(stderr.contains("unsupported output extension"));

    Ok(())
}

#[test]
fn query_returns_error_for_output_without_extension() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    let output = tmp.path().join("output_without_extension");
    create_customers_fixture(&fixture)?;

    let mut command = Command::cargo_bin("query-sheets")?;
    let assert = command
        .arg("query")
        .arg("--file")
        .arg(&fixture)
        .arg("--sql")
        .arg("SELECT CustomerId FROM Customers")
        .arg("--output")
        .arg(&output)
        .assert()
        .failure();

    let stderr = String::from_utf8(assert.get_output().stderr.clone())?;
    assert!(stderr.contains("could not detect export format"));

    Ok(())
}

#[test]
fn query_returns_error_for_sum_on_non_numeric_column() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let mut command = Command::cargo_bin("query-sheets")?;
    let assert = command
        .arg("query")
        .arg("--file")
        .arg(&fixture)
        .arg("--sql")
        .arg("SELECT Segment, SUM(CustomerName) AS Total FROM Customers GROUP BY Segment")
        .assert()
        .failure();

    let stderr = String::from_utf8(assert.get_output().stderr.clone())?;
    assert!(stderr.contains("SUM(CustomerName)"));
    assert!(stderr.contains("numeric values"));

    Ok(())
}

#[test]
fn query_returns_error_for_stddev_on_non_numeric_column() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let mut command = Command::cargo_bin("query-sheets")?;
    let assert = command
        .arg("query")
        .arg("--file")
        .arg(&fixture)
        .arg("--sql")
        .arg("SELECT Segment, STDDEV(CustomerName) AS Dispersion FROM Customers GROUP BY Segment")
        .assert()
        .failure();

    let stderr = String::from_utf8(assert.get_output().stderr.clone())?;
    assert!(stderr.contains("STDDEV(CustomerName)"));
    assert!(stderr.contains("numeric values"));

    Ok(())
}

#[test]
fn query_returns_error_for_limit_zero() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let mut command = Command::cargo_bin("query-sheets")?;
    let assert = command
        .arg("query")
        .arg("--file")
        .arg(&fixture)
        .arg("--sql")
        .arg("SELECT CustomerId FROM Customers LIMIT 0")
        .assert()
        .failure();

    let stderr = String::from_utf8(assert.get_output().stderr.clone())?;
    assert!(stderr.contains("unsupported pagination clause"));
    assert!(stderr.contains("LIMIT"));

    Ok(())
}

#[test]
fn query_returns_error_for_order_by_with_incomparable_values() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("times.xlsx");
    common::create_activity_time_fixture(&fixture)?;

    let mut command = Command::cargo_bin("query-sheets")?;
    let assert = command
        .arg("query")
        .arg("--file")
        .arg(&fixture)
        .arg("--sql")
        .arg("SELECT Time FROM Times ORDER BY Time")
        .assert()
        .failure();

    let stderr = String::from_utf8(assert.get_output().stderr.clone())?;
    assert!(stderr.contains("unsupported ORDER BY expression"));
    assert!(stderr.contains("comparable values"));

    Ok(())
}

#[test]
fn query_returns_error_for_order_by_position_out_of_range() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let mut command = Command::cargo_bin("query-sheets")?;
    let assert = command
        .arg("query")
        .arg("--file")
        .arg(&fixture)
        .arg("--sql")
        .arg("SELECT CustomerName FROM Customers ORDER BY 2")
        .assert()
        .failure();

    let stderr = String::from_utf8(assert.get_output().stderr.clone())?;
    assert!(stderr.contains("unsupported ORDER BY expression"));
    assert!(stderr.contains("out of range"));

    Ok(())
}

#[test]
fn session_returns_error_for_folder_query_without_file_alias() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("north.xlsx");
    create_customers_fixture_with_rows(
        &fixture,
        &[["N-001", "North Customer", "Enterprise", "Active"]],
    )?;

    let mut command = Command::cargo_bin("query-sheets")?;
    let assert = command
        .arg("session")
        .arg("--path")
        .arg(tmp.path())
        .write_stdin("SELECT CustomerId FROM Customers;\n.exit\n")
        .assert()
        .success();

    let stderr = String::from_utf8(assert.get_output().stderr.clone())?;
    assert!(stderr.contains("in folder mode use FROM <file_alias>.<worksheet>"));

    Ok(())
}

#[test]
fn query_returns_error_for_unsupported_full_join() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers_orders.xlsx");
    create_customers_orders_fixture(&fixture)?;

    let mut command = Command::cargo_bin("query-sheets")?;
    let assert = command
        .arg("query")
        .arg("--file")
        .arg(&fixture)
        .arg("--sql")
        .arg("SELECT c.CustomerName FROM Customers c FULL JOIN Orders o ON c.CustomerId = o.CustomerId")
        .assert()
        .failure();

    let stderr = String::from_utf8(assert.get_output().stderr.clone())?;
    assert!(stderr.contains("only simple SELECT queries are supported"));

    Ok(())
}