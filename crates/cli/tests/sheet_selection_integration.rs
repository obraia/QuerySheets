mod common;

use common::{create_multi_sheet_customers_fixture, run_cli_query};
use std::error::Error;
use tempfile::tempdir;

#[test]
fn query_uses_sheet_flag_to_select_specific_worksheet() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("multi_sheet_customers.xlsx");
    create_multi_sheet_customers_fixture(&fixture)?;

    let sql = "SELECT CustomerId, CustomerName, AccountStatus FROM Customers WHERE AccountStatus = 'Inactive'";
    let stdout = run_cli_query(&fixture, sql, Some("Archive"), true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], "CustomerId\tCustomerName\tAccountStatus");
    assert_eq!(lines[1], "A-900\tLegacy Customer\tInactive");

    Ok(())
}