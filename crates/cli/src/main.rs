use clap::{Parser, Subcommand};
use query_sheets_adapters::create_excel_source;
use query_sheets_core::{DataSource, Row, Schema, Value};
use query_sheets_query::{
    ConfiguredSqlLikeQueryEngine, QueryEngine, SqlLikeQueryEngine, TableReference,
    extract_table_name, extract_table_reference,
};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, IsTerminal, Write};
use std::path::{Path, PathBuf};

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
        #[arg(long, help = "Caminho do arquivo de saída (.csv, .json ou .jsonl)")]
        output: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            help = "Ativa comparação de texto case-sensitive no WHERE e ORDER BY"
        )]
        case_sensitive_strings: bool,
    },
    Session {
        #[arg(
            long,
            short,
            help = "Caminho para arquivo de planilha ou pasta com arquivos de planilha"
        )]
        path: String,
        #[arg(long, default_value_t = false, help = "Imprime cabeçalho da projeção")]
        header: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Ativa comparação de texto case-sensitive no WHERE e ORDER BY"
        )]
        case_sensitive_strings: bool,
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
            case_sensitive_strings,
        } => run_single_query(&file, &sql, sheet, header, output, case_sensitive_strings)?,
        Commands::Session {
            path,
            header,
            case_sensitive_strings,
        } => run_session(&path, header, case_sensitive_strings)?,
    }

    Ok(())
}

fn run_single_query(
    file: &str,
    sql: &str,
    sheet: Option<String>,
    header: bool,
    output: Option<String>,
    case_sensitive_strings: bool,
) -> Result<(), String> {
    let table_from_sql = extract_table_name(sql).map_err(|err| err.to_string())?;
    let chosen_sheet = sheet.or(table_from_sql);

    let source = create_excel_source(file, chosen_sheet.as_deref()).map_err(|err| err.to_string())?;
    let engine = SqlLikeQueryEngine.with_case_sensitive_strings(case_sensitive_strings);
    let mut execution = engine
        .execute_with_schema(source.as_ref(), sql)
        .map_err(|err| err.to_string())?;

    if let Some(output_path) = output.as_deref() {
        match detect_export_format(output_path)? {
            DetectedExportFormat::Csv => write_csv(output_path, &execution.schema, execution.rows.by_ref())?,
            DetectedExportFormat::Json => {
                write_json(output_path, &execution.schema, execution.rows.by_ref())?
            }
            DetectedExportFormat::Jsonl => {
                write_jsonl(output_path, &execution.schema, execution.rows.by_ref())?
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

    Ok(())
}

fn run_session(path: &str, header: bool, case_sensitive_strings: bool) -> Result<(), String> {
    let mut catalog = SessionCatalog::new(path)?;
    let engine = SqlLikeQueryEngine.with_case_sensitive_strings(case_sensitive_strings);

    println!("query-sheets session mode");
    println!("Digite SQL e pressione Enter. Use .exit para sair, .help para ajuda e .clear para limpar.");

    if io::stdin().is_terminal() && io::stdout().is_terminal() {
        let mut editor = DefaultEditor::new()
            .map_err(|err| format!("failed to initialize interactive editor: {err}"))?;

        loop {
            match editor.readline("query-sheets> ") {
                Ok(line) => {
                    let input = line.trim();
                    if input.is_empty() {
                        continue;
                    }

                    let _ = editor.add_history_entry(input);

                    if !process_session_input(input, header, &engine, &mut catalog) {
                        break;
                    }
                }
                Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
                Err(err) => return Err(format!("failed to read interactive input: {err}")),
            }
        }
    } else {
        let stdin = io::stdin();
        loop {
            print!("query-sheets> ");
            io::stdout()
                .flush()
                .map_err(|err| format!("failed to flush prompt: {err}"))?;

            let mut line = String::new();
            let bytes_read = stdin
                .read_line(&mut line)
                .map_err(|err| format!("failed to read input: {err}"))?;

            if bytes_read == 0 {
                break;
            }

            if !process_session_input(line.trim(), header, &engine, &mut catalog) {
                break;
            }
        }
    }

    Ok(())
}

fn process_session_input(
    input: &str,
    header: bool,
    engine: &ConfiguredSqlLikeQueryEngine,
    catalog: &mut SessionCatalog,
) -> bool {
    if input.is_empty() {
        return true;
    }

    if matches!(
        input.to_ascii_lowercase().as_str(),
        ".exit" | "exit" | "quit" | ".quit"
    ) {
        return false;
    }

    if matches!(input.to_ascii_lowercase().as_str(), ".help" | "help") {
        println!("Comandos:");
        println!("  .help  - mostra esta ajuda");
        println!("  .cache - mostra quantidade de tabelas no cache");
        println!("  .clear - limpa o console");
        println!("  .exit  - encerra o modo sessão");
        return true;
    }

    if input.eq_ignore_ascii_case(".cache") {
        println!("cached tables: {}", catalog.cache_len());
        return true;
    }

    if matches!(input.to_ascii_lowercase().as_str(), ".clear" | "clear") {
        if let Err(err) = clear_console() {
            eprintln!("erro: {err}");
        }
        return true;
    }

    let sql = input.trim_end_matches(';').trim();

    let source = match catalog.source_for_query(sql) {
        Ok(source) => source,
        Err(err) => {
            eprintln!("erro: {err}");
            return true;
        }
    };

    let mut execution = match engine.execute_with_schema(source, sql) {
        Ok(execution) => execution,
        Err(err) => {
            eprintln!("erro: {err}");
            return true;
        }
    };

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

    true
}

fn clear_console() -> Result<(), String> {
    print!("\x1B[2J\x1B[H");
    io::stdout()
        .flush()
        .map_err(|err| format!("failed to clear console: {err}"))
}

#[derive(Debug, Clone)]
struct SessionFileEntry {
    alias: String,
    file_path: PathBuf,
}

#[derive(Debug, Clone)]
enum SessionCatalogMode {
    SingleFile { file_path: PathBuf, alias: String },
    Folder { files_by_alias: HashMap<String, SessionFileEntry> },
}

#[derive(Debug, Clone)]
struct CachedTableSource {
    schema: Schema,
    rows: Vec<Row>,
}

impl DataSource for CachedTableSource {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn scan<'a>(&'a self) -> Box<dyn Iterator<Item = Row> + 'a> {
        Box::new(self.rows.iter().cloned())
    }
}

#[derive(Debug)]
struct SessionCatalog {
    mode: SessionCatalogMode,
    cache: HashMap<String, CachedTableSource>,
}

impl SessionCatalog {
    fn new(path: &str) -> Result<Self, String> {
        let resolved = PathBuf::from(path);

        if resolved.is_file() {
            let alias = file_alias(&resolved)?;
            return Ok(Self {
                mode: SessionCatalogMode::SingleFile {
                    file_path: resolved,
                    alias,
                },
                cache: HashMap::new(),
            });
        }

        if resolved.is_dir() {
            let mut files_by_alias: HashMap<String, SessionFileEntry> = HashMap::new();
            let entries = std::fs::read_dir(&resolved)
                .map_err(|err| format!("failed to read directory '{}': {err}", resolved.display()))?;

            for entry in entries {
                let entry =
                    entry.map_err(|err| format!("failed to read directory entry: {err}"))?;
                let file_path = entry.path();

                if !file_path.is_file() || !is_spreadsheet_file(&file_path) {
                    continue;
                }

                let alias = file_alias(&file_path)?;
                let alias_key = alias.to_ascii_lowercase();

                if let Some(existing) = files_by_alias.get(&alias_key) {
                    return Err(format!(
                        "duplicate file alias '{alias}' for '{}' and '{}'",
                        existing.file_path.display(),
                        file_path.display()
                    ));
                }

                files_by_alias.insert(
                    alias_key,
                    SessionFileEntry {
                        alias,
                        file_path,
                    },
                );
            }

            if files_by_alias.is_empty() {
                return Err(format!(
                    "directory '{}' has no supported spreadsheet files",
                    resolved.display()
                ));
            }

            return Ok(Self {
                mode: SessionCatalogMode::Folder { files_by_alias },
                cache: HashMap::new(),
            });
        }

        Err(format!(
            "path '{}' is not a file or directory",
            resolved.display()
        ))
    }

    fn cache_len(&self) -> usize {
        self.cache.len()
    }

    fn source_for_query(&mut self, sql: &str) -> Result<&CachedTableSource, String> {
        let table_ref = extract_table_reference(sql)
            .map_err(|err| err.to_string())?
            .ok_or_else(|| "query must include a table in FROM".to_string())?;

        let (file_alias, file_path, table_name) = self.resolve_table_reference(&table_ref)?;
        let cache_key = build_cache_key(&file_alias, &table_name);

        if !self.cache.contains_key(&cache_key) {
            let file_path_str = file_path
                .to_str()
                .ok_or_else(|| format!("invalid file path '{}'", file_path.display()))?;

            let source = create_excel_source(file_path_str, Some(&table_name))
                .map_err(|err| err.to_string())?;

            let cached = CachedTableSource {
                schema: source.schema().clone(),
                rows: source.scan().collect::<Vec<_>>(),
            };

            self.cache.insert(cache_key.clone(), cached);
        }

        self.cache
            .get(&cache_key)
            .ok_or_else(|| "internal cache error".to_string())
    }

    fn resolve_table_reference(
        &self,
        table_ref: &TableReference,
    ) -> Result<(String, PathBuf, String), String> {
        match &self.mode {
            SessionCatalogMode::SingleFile { file_path, alias } => {
                if let Some(schema) = table_ref.schema.as_deref() {
                    let file_name = file_path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("");

                    if !schema.eq_ignore_ascii_case(alias)
                        && !schema.eq_ignore_ascii_case(file_name)
                    {
                        return Err(format!(
                            "table prefix '{schema}' does not match opened file '{alias}'"
                        ));
                    }
                }

                Ok((alias.clone(), file_path.clone(), table_ref.table.clone()))
            }
            SessionCatalogMode::Folder { files_by_alias } => {
                let schema = table_ref.schema.as_deref().ok_or_else(|| {
                    "in folder mode use FROM <arquivo>.<worksheet> (dbo=tabela de arquivos)"
                        .to_string()
                })?;

                let alias_key = schema.to_ascii_lowercase();
                let file_entry = files_by_alias.get(&alias_key).ok_or_else(|| {
                    let mut aliases = files_by_alias
                        .values()
                        .map(|entry| entry.alias.clone())
                        .collect::<Vec<_>>();
                    aliases.sort();

                    format!(
                        "file alias '{schema}' not found. Available aliases: {}",
                        aliases.join(", ")
                    )
                })?;

                Ok((
                    file_entry.alias.clone(),
                    file_entry.file_path.clone(),
                    table_ref.table.clone(),
                ))
            }
        }
    }
}

fn file_alias(path: &Path) -> Result<String, String> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(|value| value.to_string())
        .ok_or_else(|| format!("could not derive file alias from '{}'", path.display()))
}

fn is_spreadsheet_file(path: &Path) -> bool {
    let Some(extension) = path.extension().and_then(|ext| ext.to_str()) else {
        return false;
    };

    matches!(
        extension.to_ascii_lowercase().as_str(),
        "xlsx" | "xlsm" | "xls" | "xlsb" | "ods"
    )
}

fn build_cache_key(file_alias: &str, table_name: &str) -> String {
    format!(
        "{}::{}",
        file_alias.to_ascii_lowercase(),
        table_name.to_ascii_lowercase()
    )
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
    let mut writer = BufWriter::new(file);

    writer
        .write_all(b"[")
        .map_err(|err| format!("failed writing JSON array start: {err}"))?;

    let mut first = true;
    for row in rows {
        if !first {
            writer
                .write_all(b",")
                .map_err(|err| format!("failed writing JSON separator: {err}"))?;
        }

        first = false;

        let value = serde_json::Value::Object(row_to_json_object(schema, &row));
        serde_json::to_writer(&mut writer, &value)
            .map_err(|err| format!("failed writing JSON object: {err}"))?;
    }

    writer
        .write_all(b"]")
        .map_err(|err| format!("failed writing JSON array end: {err}"))?;

    writer
        .flush()
        .map_err(|err| format!("failed flushing JSON output: {err}"))?;

    Ok(())
}

fn write_jsonl(
    path: &str,
    schema: &Schema,
    rows: impl Iterator<Item = query_sheets_core::Row>,
) -> Result<(), String> {
    let file = File::create(Path::new(path))
        .map_err(|err| format!("failed to create output file '{path}': {err}"))?;
    let mut writer = BufWriter::new(file);

    for row in rows {
        let value = serde_json::Value::Object(row_to_json_object(schema, &row));
        serde_json::to_writer(&mut writer, &value)
            .map_err(|err| format!("failed writing JSONL row: {err}"))?;
        writer
            .write_all(b"\n")
            .map_err(|err| format!("failed writing JSONL line break: {err}"))?;
    }

    writer
        .flush()
        .map_err(|err| format!("failed flushing JSONL output: {err}"))?;

    Ok(())
}

fn row_to_json_object(schema: &Schema, row: &query_sheets_core::Row) -> serde_json::Map<String, serde_json::Value> {
    let mut object = serde_json::Map::with_capacity(schema.columns.len());

    for (idx, column) in schema.columns.iter().enumerate() {
        let value = row.values.get(idx).unwrap_or(&Value::Null);
        object.insert(column.name.clone(), value_to_json(value));
    }

    object
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
    Jsonl,
}

fn detect_export_format(output_path: &str) -> Result<DetectedExportFormat, String> {
    let extension = Path::new(output_path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase())
        .ok_or_else(|| {
            format!(
                "could not detect export format from '{output_path}'. Use an output file ending with .csv, .json or .jsonl"
            )
        })?;

    match extension.as_str() {
        "csv" => Ok(DetectedExportFormat::Csv),
        "json" => Ok(DetectedExportFormat::Json),
        "jsonl" => Ok(DetectedExportFormat::Jsonl),
        _ => Err(format!(
            "unsupported output extension '.{extension}'. Supported extensions: .csv, .json, .jsonl"
        )),
    }
}
