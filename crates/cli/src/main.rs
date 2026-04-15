use clap::{Parser, Subcommand};
use query_sheets_adapters::create_excel_source;
use query_sheets_core::Value;
use query_sheets_query::{QueryEngine, SqlLikeQueryEngine, extract_table_name};

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
        } => {
            let table_from_sql = extract_table_name(&sql).map_err(|err| err.to_string())?;
            let chosen_sheet = sheet.or(table_from_sql);

            let source = create_excel_source(&file, chosen_sheet.as_deref()).map_err(|err| err.to_string())?;
            let engine = SqlLikeQueryEngine;
            let rows = engine
                .execute(source.as_ref(), &sql)
                .map_err(|err| err.to_string())?;

            for row in rows {
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
