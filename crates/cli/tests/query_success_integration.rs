mod common;

use common::{
    create_activity_time_fixture, create_customers_fixture, create_customers_fixture_with_rows,
    create_customers_orders_fixture, create_sales_fixture, run_cli_query,
    run_cli_query_with_case_sensitive_strings,
    run_cli_session,
};
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

#[test]
fn query_outputs_group_by_count_header_and_rows() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let sql = "SELECT Segment, COUNT(*) AS TotalCustomers FROM Customers GROUP BY Segment";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "Segment\tTotalCustomers");
    assert_eq!(lines[1], "Enterprise\t2");
    assert_eq!(lines[2], "SMB\t1");

    Ok(())
}

#[test]
fn query_outputs_group_by_count_sum_avg_header_and_rows() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("sales.xlsx");
    create_sales_fixture(&fixture)?;

    let sql =
        "SELECT Segment, COUNT(*) AS TotalRows, SUM(Revenue) AS TotalRevenue, AVG(Revenue) AS AvgRevenue FROM Sales GROUP BY Segment";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "Segment\tTotalRows\tTotalRevenue\tAvgRevenue");
    assert_eq!(lines[1], "Enterprise\t2\t211\t105.5");
    assert_eq!(lines[2], "SMB\t1\t50\t50");

    Ok(())
}

#[test]
fn query_outputs_group_by_min_max_header_and_rows() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("sales.xlsx");
    create_sales_fixture(&fixture)?;

    let sql =
        "SELECT Segment, MIN(Revenue) AS MinRevenue, MAX(Revenue) AS MaxRevenue FROM Sales GROUP BY Segment";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "Segment\tMinRevenue\tMaxRevenue");
    assert_eq!(lines[1], "Enterprise\t91\t120");
    assert_eq!(lines[2], "SMB\t50\t50");

    Ok(())
}

#[test]
fn query_outputs_casted_group_by_aggregates_with_mixed_Time_values() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("times.xlsx");
    create_activity_time_fixture(&fixture)?;

    let sql = "SELECT Activity, COUNT(*) AS Quantity, AVG(CAST(Time AS FLOAT)) AS AvgTime, SUM(CAST(Time AS FLOAT)) AS TotalTime, MIN(CAST(Time AS FLOAT)) AS MinTime, MAX(CAST(Time AS FLOAT)) AS MaxTime FROM Times GROUP BY Activity";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(
        lines[0],
        "Activity\tQuantity\tAvgTime\tTotalTime\tMinTime\tMaxTime"
    );
    assert_eq!(lines[1], "A\t3\t15\t30\t10\t20");
    assert_eq!(lines[2], "B\t2\t5.5\t5.5\t5.5\t5.5");

    Ok(())
}

#[test]
fn query_outputs_group_by_count_column_with_cast_expression() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("times.xlsx");
    create_activity_time_fixture(&fixture)?;

    let sql = "SELECT Activity, COUNT(CAST(Time AS FLOAT)) AS NumericTimeRows FROM Times GROUP BY Activity";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "Activity\tNumericTimeRows");
    assert_eq!(lines[1], "A\t2");
    assert_eq!(lines[2], "B\t1");

    Ok(())
}

#[test]
fn query_outputs_group_by_stddev_with_cast_expression() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("times.xlsx");
    create_activity_time_fixture(&fixture)?;

    let sql = "SELECT Activity, STDDEV(CAST(Time AS FLOAT)) AS StdTime FROM Times GROUP BY Activity";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "Activity\tStdTime");
    assert_eq!(lines[1], "A\t5");
    assert_eq!(lines[2], "B\t0");

    Ok(())
}

#[test]
fn query_outputs_rows_with_limit_and_offset() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let sql = "SELECT CustomerId, CustomerName FROM Customers LIMIT 1 OFFSET 1";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], "CustomerId\tCustomerName");
    assert_eq!(lines[1], "C-002\tBob Smith");

    Ok(())
}

#[test]
fn query_outputs_rows_ordered_with_limit_and_offset() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let sql = "SELECT CustomerId, CustomerName FROM Customers ORDER BY CustomerName DESC LIMIT 2 OFFSET 0";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "CustomerId\tCustomerName");
    assert_eq!(lines[1], "C-003\tCarla Davis");
    assert_eq!(lines[2], "C-002\tBob Smith");

    Ok(())
}

#[test]
fn query_outputs_rows_ordered_by_non_projected_column() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let sql = "SELECT CustomerName FROM Customers ORDER BY CustomerId DESC";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0], "CustomerName");
    assert_eq!(lines[1], "Carla Davis");
    assert_eq!(lines[2], "Bob Smith");
    assert_eq!(lines[3], "Alice Johnson");

    Ok(())
}

#[test]
fn query_outputs_rows_with_positional_order_by() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let sql = "SELECT CustomerId, CustomerName FROM Customers ORDER BY 2 DESC LIMIT 2";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "CustomerId\tCustomerName");
    assert_eq!(lines[1], "C-003\tCarla Davis");
    assert_eq!(lines[2], "C-002\tBob Smith");

    Ok(())
}

#[test]
fn query_outputs_rows_with_case_insensitive_where_string_match() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let sql = "SELECT CustomerId, Segment FROM Customers WHERE Segment = 'enterprise'";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "CustomerId\tSegment");
    assert_eq!(lines[1], "C-001\tEnterprise");
    assert_eq!(lines[2], "C-003\tEnterprise");

    Ok(())
}

#[test]
fn query_outputs_rows_with_case_sensitive_strings_flag() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let sql = "SELECT CustomerId, Segment FROM Customers WHERE Segment = 'enterprise'";
    let stdout = run_cli_query_with_case_sensitive_strings(&fixture, sql, None, true, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0], "CustomerId\tSegment");

    Ok(())
}

#[test]
fn query_outputs_rows_with_inner_join_between_worksheets() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers_orders.xlsx");
    create_customers_orders_fixture(&fixture)?;

    let sql = "SELECT c.CustomerName, o.Amount FROM Customers c JOIN Orders o ON c.CustomerId = o.CustomerId ORDER BY o.Amount";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0], "CustomerName\tAmount");
    assert_eq!(lines[1], "Bob\t90");
    assert_eq!(lines[2], "Alice\t150");
    assert_eq!(lines[3], "Alice\t210");

    Ok(())
}

#[test]
fn query_outputs_rows_with_left_join_unmatched_row() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers_orders.xlsx");
    create_customers_orders_fixture(&fixture)?;

    let sql = "SELECT c.CustomerName FROM Customers c LEFT JOIN Orders o ON c.CustomerId = o.CustomerId WHERE c.CustomerId = 'C-003'";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], "CustomerName");
    assert_eq!(lines[1], "Carla");

    Ok(())
}

#[test]
fn query_outputs_rows_with_right_join_unmatched_row() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers_orders.xlsx");
    create_customers_orders_fixture(&fixture)?;

    let sql = "SELECT o.OrderId FROM Customers c RIGHT JOIN Orders o ON c.CustomerId = o.CustomerId WHERE o.CustomerId = 'C-999'";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], "OrderId");
    assert_eq!(lines[1], "O-999");

    Ok(())
}

#[test]
fn session_executes_incremental_queries_against_single_file() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let input = "SELECT CustomerId FROM Customers WHERE Segment = 'Enterprise';\nSELECT CustomerName FROM Customers LIMIT 1;\n.exit\n";
    let (stdout, stderr) = run_cli_session(&fixture, input, true, false)?;

    assert!(stderr.trim().is_empty());
    assert!(stdout.contains("query-sheets session mode"));
    assert!(stdout.contains("CustomerId"));
    assert!(stdout.contains("C-001"));
    assert!(stdout.contains("C-003"));
    assert!(stdout.contains("CustomerName"));
    assert!(stdout.contains("Alice Johnson"));

    Ok(())
}

#[test]
fn session_resolves_folder_queries_as_file_alias_and_worksheet() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let north = tmp.path().join("north.xlsx");
    let south = tmp.path().join("south.xlsx");

    create_customers_fixture_with_rows(
        &north,
        &[["N-001", "North Customer", "Enterprise", "Active"]],
    )?;
    create_customers_fixture_with_rows(
        &south,
        &[["S-001", "South Customer", "SMB", "Inactive"]],
    )?;

    let input = "SELECT CustomerId, CustomerName FROM north.Customers WHERE AccountStatus = 'Active';\nSELECT CustomerId FROM south.Customers WHERE AccountStatus = 'Inactive';\n.exit\n";
    let (stdout, stderr) = run_cli_session(tmp.path(), input, false, false)?;

    assert!(stderr.trim().is_empty());
    assert!(stdout.contains("N-001\tNorth Customer"));
    assert!(stdout.contains("S-001"));

    Ok(())
}

#[test]
fn session_cache_command_reflects_loaded_tables() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let input = ".cache\nSELECT CustomerId FROM Customers LIMIT 1;\n.cache\n.exit\n";
    let (stdout, stderr) = run_cli_session(&fixture, input, false, false)?;

    assert!(stderr.trim().is_empty());
    assert!(stdout.contains("cached tables: 0"));
    assert!(stdout.contains("cached tables: 1"));

    Ok(())
}

#[test]
fn session_clear_command_emits_console_clear_sequence() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("customers.xlsx");
    create_customers_fixture(&fixture)?;

    let input = ".clear\n.exit\n";
    let (stdout, stderr) = run_cli_session(&fixture, input, false, false)?;

    assert!(stderr.trim().is_empty());
    assert!(stdout.contains("\u{1b}[2J\u{1b}[H"));

    Ok(())
}