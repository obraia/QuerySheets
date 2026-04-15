mod common;

use common::{create_customers_fixture, run_cli_query};
use std::error::Error;
use tempfile::tempdir;

#[test]
fn query_outputs_alias_header_and_filtered_rows() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let sql = "SELECT CustomerId AS ClientId, CustomerName, AccountStatus FROM Customers WHERE Segment = 'Enterprise'";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "ClientId\tCustomerName\tAccountStatus");
    assert_eq!(lines[1], "C-001\tAlice Johnson\tActive");
    assert_eq!(lines[2], "C-003\tCarla Davis\tChurned");

    Ok(())
}

#[test]
fn query_outputs_expression_header_and_value() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let sql = "SELECT CustomerId AS ClientId, 1 + 2 AS PriorityScore, Segment FROM Customers WHERE CustomerId = 'C-001'";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], "ClientId\tPriorityScore\tSegment");
    assert_eq!(lines[1], "C-001\t3\tEnterprise");

    Ok(())
}

#[test]
fn query_outputs_wildcard_header_and_rows() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let sql = "SELECT * FROM Customers WHERE AccountStatus = 'Active'";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "CustomerId\tCustomerName\tSegment\tAccountStatus");
    assert_eq!(lines[1], "C-001\tAlice Johnson\tEnterprise\tActive");
    assert_eq!(lines[2], "C-002\tBob Smith\tSMB\tActive");

    Ok(())
}