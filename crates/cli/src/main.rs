use clap::{Parser, Subcommand};
use query_sheets_adapters::create_excel_source;
use query_sheets_core::{Schema, Value};
use query_sheets_query::{QueryEngine, SqlLikeQueryEngine, extract_table_name};
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

#[derive(Debug, Parser)]
#[command(name = "query-sheets")]
#[command(about = "CLI para consultar arquivos Excel com SQL-like")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Query {
        #[arg(short, long, help = "Caminho para o arquivo .xlsx")]
        file: String,
        #[arg(short = 'q', long, help = "Consulta SQL-like")]
        sql: String,
        #[arg(short, long, help = "Nome da planilha (sobrescreve o FROM da query)")]
        sheet: Option<String>,
        #[arg(long, default_value_t = false, help = "Imprime cabeçalho da projeção")]
        header: bool,
        #[arg(long, help = "Caminho do arquivo de saída (.csv ou .json)")]
        output: Option<String>,
    },
}

fn main() {
    if let Err(err) = run() {
        eprintln!("erro: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Query {
            file,
            sql,
            sheet,
            header,
            output,
        } => {
            let table_from_sql = extract_table_name(&sql).map_err(|err| err.to_string())?;
            let chosen_sheet = sheet.or(table_from_sql);

            let source = create_excel_source(&file, chosen_sheet.as_deref()).map_err(|err| err.to_string())?;
            let engine = SqlLikeQueryEngine;
            let mut execution = engine
                .execute_with_schema(source.as_ref(), &sql)
                .map_err(|err| err.to_string())?;

            if let Some(output_path) = output.as_deref() {
                match detect_export_format(output_path)? {
                    DetectedExportFormat::Csv => {
                        write_csv(output_path, &execution.schema, execution.rows.by_ref())?
                    }
                    DetectedExportFormat::Json => {
                        write_json(output_path, &execution.schema, execution.rows.by_ref())?
                    }
                }

                return Ok(());
            }

            if header {
                let headers = execution
                    .schema
                    .columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>()
                    .join("\t");
                println!("{headers}");
            }

            for row in execution.rows.by_ref() {
                let line = row
                    .values
                    .iter()
                    .map(value_to_string)
                    .collect::<Vec<_>>()
                    .join("\t");
                println!("{line}");
            }
        }
    }

    Ok(())
}

fn value_to_string(value: &Value) -> String {
    value.to_string()
}

fn write_csv(
    path: &str,
    schema: &Schema,
    rows: impl Iterator<Item = query_sheets_core::Row>,
) -> Result<(), String> {
    let file = File::create(Path::new(path))
        .map_err(|err| format!("failed to create output file '{path}': {err}"))?;
    let writer = BufWriter::new(file);
    let mut csv_writer = csv::Writer::from_writer(writer);

    let headers = schema
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    csv_writer
        .write_record(headers)
        .map_err(|err| format!("failed writing CSV header: {err}"))?;

    for row in rows {
        let record = row.values.iter().map(value_to_string).collect::<Vec<_>>();
        csv_writer
            .write_record(record)
            .map_err(|err| format!("failed writing CSV row: {err}"))?;
    }

    csv_writer
        .flush()
        .map_err(|err| format!("failed flushing CSV output: {err}"))?;

    Ok(())
}

fn write_json(
    path: &str,
    schema: &Schema,
    rows: impl Iterator<Item = query_sheets_core::Row>,
) -> Result<(), String> {
    let file = File::create(Path::new(path))
        .map_err(|err| format!("failed to create output file '{path}': {err}"))?;
    let writer = BufWriter::new(file);

    let mut objects = Vec::new();
    for row in rows {
        let mut object = serde_json::Map::with_capacity(schema.columns.len());

        for (idx, column) in schema.columns.iter().enumerate() {
            let value = row.values.get(idx).unwrap_or(&Value::Null);
            object.insert(column.name.clone(), value_to_json(value));
        }

        objects.push(serde_json::Value::Object(object));
    }

    serde_json::to_writer_pretty(writer, &objects)
        .map_err(|err| format!("failed writing JSON output: {err}"))?;

    Ok(())
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Int(v) => serde_json::Value::Number((*v).into()),
        Value::Float(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(v) => serde_json::Value::String(v.clone()),
        Value::Bool(v) => serde_json::Value::Bool(*v),
        Value::Null => serde_json::Value::Null,
    }
}

#[derive(Debug, Clone, Copy)]
enum DetectedExportFormat {
    Csv,
    Json,
}

fn detect_export_format(output_path: &str) -> Result<DetectedExportFormat, String> {
    let extension = Path::new(output_path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase())
        .ok_or_else(|| {
            format!(
                "could not detect export format from '{output_path}'. Use an output file ending with .csv or .json"
            )
        })?;

    match extension.as_str() {
        "csv" => Ok(DetectedExportFormat::Csv),
        "json" => Ok(DetectedExportFormat::Json),
        _ => Err(format!(
            "unsupported output extension '.{extension}'. Supported extensions: .csv, .json"
        )),
    }
}
