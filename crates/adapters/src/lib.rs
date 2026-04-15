use query_sheets_core::DataSource;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("feature 'calamine' is disabled")]
    CalamineDisabled,
    #[error("could not open workbook '{path}': {reason}")]
    WorkbookOpen { path: String, reason: String },
    #[error("worksheet '{sheet}' was not found")]
    WorksheetNotFound { sheet: String },
    #[error("could not read worksheet '{sheet}': {reason}")]
    WorksheetRead { sheet: String, reason: String },
    #[error("workbook has no worksheets")]
    EmptyWorkbook,
}

#[cfg(feature = "calamine")]
pub use excel::CalamineExcelSource;

pub fn create_excel_source(
    path: &str,
    sheet: Option<&str>,
) -> Result<Box<dyn DataSource>, AdapterError> {
    #[cfg(feature = "calamine")]
    {
        let source = CalamineExcelSource::new(path, sheet)?;
        return Ok(Box::new(source));
    }

    #[allow(unreachable_code)]
    Err(AdapterError::CalamineDisabled)
}

#[cfg(feature = "calamine")]
mod excel {
    use super::AdapterError;
    use calamine::{Data, Range, Reader, open_workbook_auto};
    use query_sheets_core::{Column, DataSource, Row, Schema, Value};

    pub struct CalamineExcelSource {
        schema: Schema,
        range: Range<Data>,
    }

    impl CalamineExcelSource {
        pub fn new(path: &str, sheet_name: Option<&str>) -> Result<Self, AdapterError> {
            let mut workbook = open_workbook_auto(path).map_err(|err| AdapterError::WorkbookOpen {
                path: path.to_string(),
                reason: err.to_string(),
            })?;

            let chosen_sheet = if let Some(name) = sheet_name {
                name.to_string()
            } else {
                workbook
                    .sheet_names()
                    .first()
                    .cloned()
                    .ok_or(AdapterError::EmptyWorkbook)?
            };

            let range = workbook
                .worksheet_range(&chosen_sheet)
                .map_err(|err| AdapterError::WorksheetRead {
                    sheet: chosen_sheet.clone(),
                    reason: err.to_string(),
                })?;

            let mut rows_iter = range.rows();
            let header = rows_iter
                .next()
                .ok_or(AdapterError::WorksheetNotFound {
                    sheet: chosen_sheet.clone(),
                })?;

            let schema = Schema::new(
                header
                    .iter()
                    .enumerate()
                    .map(|(idx, cell)| {
                        let name = match cell {
                            Data::String(value) if !value.trim().is_empty() => value.trim().to_owned(),
                            _ => format!("col_{}", idx + 1),
                        };
                        Column::new(name)
                    })
                    .collect(),
            );

            Ok(Self { schema, range })
        }
    }

    impl DataSource for CalamineExcelSource {
        fn schema(&self) -> &Schema {
            &self.schema
        }

        fn scan<'a>(&'a self) -> Box<dyn Iterator<Item = Row> + 'a> {
            let schema_len = self.schema.columns.len();
            Box::new(self.range.rows().skip(1).map(move |cells| {
                let mut values = Vec::with_capacity(schema_len);

                for idx in 0..schema_len {
                    let value = cells.get(idx).map(map_cell).unwrap_or(Value::Null);
                    values.push(value);
                }

                Row::new(values)
            }))
        }
    }

    fn map_cell(cell: &Data) -> Value {
        match cell {
            Data::Int(v) => Value::Int(*v),
            Data::Float(v) => Value::Float(*v),
            Data::String(v) => Value::String(v.clone()),
            Data::Bool(v) => Value::Bool(*v),
            Data::DateTime(v) => Value::String(v.to_string()),
            Data::DateTimeIso(v) => Value::String(v.clone()),
            Data::DurationIso(v) => Value::String(v.clone()),
            Data::Error(v) => Value::String(v.to_string()),
            Data::Empty => Value::Null,
        }
    }
}
