# QuerySheets Studio (Tauri)

Desktop UI option for QuerySheets. This app is intentionally separate from the CLI so it can be shipped as an optional installer component.

## Features in this bootstrap

- Modern desktop layout inspired by code editors
- Open a folder with spreadsheets
- Explore workbook aliases and worksheet names
- Run SQL queries against multiple files using file alias + worksheet syntax
- Works with JOIN support from QuerySheets engine

## Query syntax in folder mode

Use the file alias (filename without extension) as schema:

SELECT c.CustomerName, o.Amount
FROM example.Customers c
LEFT JOIN example.Orders o ON c.CustomerId = o.CustomerId
LIMIT 100

## Run in dev mode

1. Install frontend dependencies:

npm install

2. Start Tauri app:

npm run tauri:dev

## Build desktop app

npm run tauri:build

## Notes

- This app uses the existing workspace crates as local path dependencies.
- Results are capped to 2000 rows in the UI command to keep interaction responsive.
