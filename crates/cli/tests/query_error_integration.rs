mod common;

use assert_cmd::Command;
use common::create_customers_fixture;
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