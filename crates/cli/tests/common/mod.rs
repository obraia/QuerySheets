#![allow(dead_code)]

use assert_cmd::Command;
use rust_xlsxwriter::Workbook;
use std::error::Error;
use std::path::Path;

pub fn create_customers_fixture(path: &Path) -> Result<(), Box<dyn Error>> {
    let rows = [
        ["C-001", "Alice Johnson", "Enterprise", "Active"],
        ["C-002", "Bob Smith", "SMB", "Active"],
        ["C-003", "Carla Davis", "Enterprise", "Churned"],
    ];

    create_customers_fixture_with_rows(path, &rows)
}

pub fn create_customers_fixture_with_rows(
    path: &Path,
    rows: &[[&str; 4]],
) -> Result<(), Box<dyn Error>> {
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    worksheet.set_name("Customers")?;

    worksheet.write_string(0, 0, "CustomerId")?;
    worksheet.write_string(0, 1, "CustomerName")?;
    worksheet.write_string(0, 2, "Segment")?;
    worksheet.write_string(0, 3, "AccountStatus")?;

    for (offset, row) in rows.iter().enumerate() {
        let row_idx = (offset + 1) as u32;
        worksheet.write_string(row_idx, 0, row[0])?;
        worksheet.write_string(row_idx, 1, row[1])?;
        worksheet.write_string(row_idx, 2, row[2])?;
        worksheet.write_string(row_idx, 3, row[3])?;
    }

    workbook.save(path)?;
    Ok(())
}

pub fn create_sales_fixture(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    worksheet.set_name("Sales")?;

    worksheet.write_string(0, 0, "Segment")?;
    worksheet.write_string(0, 1, "Revenue")?;

    worksheet.write_string(1, 0, "Enterprise")?;
    worksheet.write_number(1, 1, 120.0)?;

    worksheet.write_string(2, 0, "Enterprise")?;
    worksheet.write_number(2, 1, 91.0)?;

    worksheet.write_string(3, 0, "SMB")?;
    worksheet.write_number(3, 1, 50.0)?;

    workbook.save(path)?;
    Ok(())
}

pub fn create_activity_time_fixture(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    worksheet.set_name("Times")?;

    worksheet.write_string(0, 0, "Activity")?;
    worksheet.write_string(0, 1, "Time")?;

    worksheet.write_string(1, 0, "A")?;
    worksheet.write_number(1, 1, 10.0)?;

    worksheet.write_string(2, 0, "A")?;
    worksheet.write_string(2, 1, "-")?;

    worksheet.write_string(3, 0, "A")?;
    worksheet.write_string(3, 1, "20")?;

    worksheet.write_string(4, 0, "B")?;
    worksheet.write_string(4, 1, "5.5")?;

    worksheet.write_string(5, 0, "B")?;
    worksheet.write_string(5, 1, "n/a")?;

    workbook.save(path)?;
    Ok(())
}

pub fn create_multi_sheet_customers_fixture(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut workbook = Workbook::new();

    let customers = workbook.add_worksheet();
    customers.set_name("Customers")?;
    customers.write_string(0, 0, "CustomerId")?;
    customers.write_string(0, 1, "CustomerName")?;
    customers.write_string(0, 2, "Segment")?;
    customers.write_string(0, 3, "AccountStatus")?;
    customers.write_string(1, 0, "C-100")?;
    customers.write_string(1, 1, "Primary Customer")?;
    customers.write_string(1, 2, "Enterprise")?;
    customers.write_string(1, 3, "Active")?;

    let archive = workbook.add_worksheet();
    archive.set_name("Archive")?;
    archive.write_string(0, 0, "CustomerId")?;
    archive.write_string(0, 1, "CustomerName")?;
    archive.write_string(0, 2, "Segment")?;
    archive.write_string(0, 3, "AccountStatus")?;
    archive.write_string(1, 0, "A-900")?;
    archive.write_string(1, 1, "Legacy Customer")?;
    archive.write_string(1, 2, "SMB")?;
    archive.write_string(1, 3, "Inactive")?;
    archive.write_string(2, 0, "A-901")?;
    archive.write_string(2, 1, "Historical Account")?;
    archive.write_string(2, 2, "Enterprise")?;
    archive.write_string(2, 3, "Active")?;

    workbook.save(path)?;
    Ok(())
}

pub fn create_customers_orders_fixture(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut workbook = Workbook::new();

    let customers = workbook.add_worksheet();
    customers.set_name("Customers")?;
    customers.write_string(0, 0, "CustomerId")?;
    customers.write_string(0, 1, "CustomerName")?;
    customers.write_string(1, 0, "C-001")?;
    customers.write_string(1, 1, "Alice")?;
    customers.write_string(2, 0, "C-002")?;
    customers.write_string(2, 1, "Bob")?;
    customers.write_string(3, 0, "C-003")?;
    customers.write_string(3, 1, "Carla")?;

    let orders = workbook.add_worksheet();
    orders.set_name("Orders")?;
    orders.write_string(0, 0, "OrderId")?;
    orders.write_string(0, 1, "CustomerId")?;
    orders.write_string(0, 2, "Amount")?;
    orders.write_string(1, 0, "O-100")?;
    orders.write_string(1, 1, "C-001")?;
    orders.write_number(1, 2, 150.0)?;
    orders.write_string(2, 0, "O-101")?;
    orders.write_string(2, 1, "C-002")?;
    orders.write_number(2, 2, 90.0)?;
    orders.write_string(3, 0, "O-102")?;
    orders.write_string(3, 1, "C-001")?;
    orders.write_number(3, 2, 210.0)?;
    orders.write_string(4, 0, "O-999")?;
    orders.write_string(4, 1, "C-999")?;
    orders.write_number(4, 2, 42.0)?;

    workbook.save(path)?;
    Ok(())
}

pub fn run_cli_query(
    file: &Path,
    sql: &str,
    sheet: Option<&str>,
    header: bool,
) -> Result<String, Box<dyn Error>> {
    let mut command = Command::cargo_bin("query-sheets")?;
    command
        .arg("query")
        .arg("--file")
        .arg(file)
        .arg("--sql")
        .arg(sql);

    if let Some(sheet_name) = sheet {
        command.arg("--sheet").arg(sheet_name);
    }

    if header {
        command.arg("--header");
    }

    let assert = command.assert().success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone())?;
    Ok(stdout)
}

pub fn run_cli_query_with_case_sensitive_strings(
    file: &Path,
    sql: &str,
    sheet: Option<&str>,
    header: bool,
    case_sensitive_strings: bool,
) -> Result<String, Box<dyn Error>> {
    let mut command = Command::cargo_bin("query-sheets")?;
    command
        .arg("query")
        .arg("--file")
        .arg(file)
        .arg("--sql")
        .arg(sql);

    if let Some(sheet_name) = sheet {
        command.arg("--sheet").arg(sheet_name);
    }

    if header {
        command.arg("--header");
    }

    if case_sensitive_strings {
        command.arg("--case-sensitive-strings");
    }

    let assert = command.assert().success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone())?;
    Ok(stdout)
}

pub fn run_cli_session(
    path: &Path,
    input: &str,
    header: bool,
    case_sensitive_strings: bool,
) -> Result<(String, String), Box<dyn Error>> {
    let mut command = Command::cargo_bin("query-sheets")?;
    command.arg("session").arg("--path").arg(path);

    if header {
        command.arg("--header");
    }

    if case_sensitive_strings {
        command.arg("--case-sensitive-strings");
    }

    let assert = command.write_stdin(input).assert().success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone())?;
    let stderr = String::from_utf8(assert.get_output().stderr.clone())?;

    Ok((stdout, stderr))
}

pub fn run_cli_export(
    file: &Path,
    sql: &str,
    sheet: Option<&str>,
    output_path: &Path,
) -> Result<(), Box<dyn Error>> {
    let mut command = Command::cargo_bin("query-sheets")?;
    command
        .arg("query")
        .arg("--file")
        .arg(file)
        .arg("--sql")
        .arg(sql)
        .arg("--output")
        .arg(output_path);

    if let Some(sheet_name) = sheet {
        command.arg("--sheet").arg(sheet_name);
    }

    command.assert().success();
    Ok(())
}