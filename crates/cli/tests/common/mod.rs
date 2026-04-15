#![allow(dead_code)]

use assert_cmd::Command;
use rust_xlsxwriter::Workbook;
use std::error::Error;
use std::path::Path;

pub fn create_customers_fixture(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    worksheet.set_name("Customers")?;

    worksheet.write_string(0, 0, "CustomerId")?;
    worksheet.write_string(0, 1, "CustomerName")?;
    worksheet.write_string(0, 2, "Segment")?;
    worksheet.write_string(0, 3, "AccountStatus")?;

    let rows = [
        ["C-001", "Alice Johnson", "Enterprise", "Active"],
        ["C-002", "Bob Smith", "SMB", "Active"],
        ["C-003", "Carla Davis", "Enterprise", "Churned"],
    ];

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