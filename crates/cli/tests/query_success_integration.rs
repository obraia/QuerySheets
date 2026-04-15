mod common;

use common::{
    create_activity_time_fixture, create_customers_fixture, create_sales_fixture, run_cli_query,
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
fn query_outputs_casted_group_by_aggregates_with_mixed_tempo_values() -> Result<(), Box<dyn Error>> {
    let tmp = tempdir()?;
    let fixture = tmp.path().join("times.xlsx");
    create_activity_time_fixture(&fixture)?;

    let sql = "SELECT Atividade, COUNT(*) AS Qtde, AVG(CAST(Tempo AS FLOAT)) AS TempoMedio, SUM(CAST(Tempo AS FLOAT)) AS TempoTotal, MIN(CAST(Tempo AS FLOAT)) AS TempoMinimo, MAX(CAST(Tempo AS FLOAT)) AS TempoMaximo FROM Times GROUP BY Atividade";
    let stdout = run_cli_query(&fixture, sql, None, true)?;
    let lines = stdout.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 3);
    assert_eq!(
        lines[0],
        "Atividade\tQtde\tTempoMedio\tTempoTotal\tTempoMinimo\tTempoMaximo"
    );
    assert_eq!(lines[1], "A\t3\t15\t30\t10\t20");
    assert_eq!(lines[2], "B\t2\t5.5\t5.5\t5.5\t5.5");

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