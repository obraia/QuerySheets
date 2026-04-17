use clap::{Parser, Subcommand, ValueEnum};
use query_sheets_adapters::create_excel_source;
use query_sheets_core::{DataSource, Row, Schema, Value};
use query_sheets_query::{
    ConfiguredSqlLikeQueryEngine, QueryEngine, QueryError as QueryExecutionError,
    ResolvedTableData, SqlLikeQueryEngine, TableReference, extract_table_reference,
};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, IsTerminal, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Parser)]
#[command(name = "query-sheets")]
#[command(about = "CLI to query spreadsheet files with SQL-like syntax")]
struct Cli {
    #[arg(
        long,
        value_enum,
        default_value_t = CliLanguage::En,
        global = true,
        help = "CLI language"
    )]
    lang: CliLanguage,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliLanguage {
    En,
    #[value(name = "pt-BR")]
    PtBr,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Query {
        #[arg(short, long, help = "Path to the spreadsheet file")]
        file: String,
        #[arg(short = 'q', long, help = "SQL-like query")]
        sql: String,
        #[arg(short, long, help = "Worksheet name (overrides query FROM table)")]
        sheet: Option<String>,
        #[arg(long, default_value_t = false, help = "Print projection header")]
        header: bool,
        #[arg(long, help = "Output path (.csv, .json, or .jsonl)")]
        output: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            help = "Enable case-sensitive string comparison in WHERE and ORDER BY"
        )]
        case_sensitive_strings: bool,
    },
    Session {
        #[arg(
            long,
            short,
            help = "Path to a spreadsheet file or a folder with spreadsheet files"
        )]
        path: String,
        #[arg(long, default_value_t = false, help = "Print projection header")]
        header: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Enable case-sensitive string comparison in WHERE and ORDER BY"
        )]
        case_sensitive_strings: bool,
    },
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();
    let lang = cli.lang;

    match cli.command {
        Commands::Query {
            file,
            sql,
            sheet,
            header,
            output,
            case_sensitive_strings,
        } => run_single_query(
            &file,
            &sql,
            sheet,
            header,
            output,
            case_sensitive_strings,
            lang,
        )?,
        Commands::Session {
            path,
            header,
            case_sensitive_strings,
        } => run_session(&path, header, case_sensitive_strings, lang)?,
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
    lang: CliLanguage,
) -> Result<(), String> {
    let mut catalog = SessionCatalog::new(file, lang)?;

    let primary_source = if let Some(sheet_name) = sheet {
        let source = create_excel_source(file, Some(&sheet_name)).map_err(|err| err.to_string())?;
        CachedTableSource {
            schema: source.schema().clone(),
            rows: source.scan().collect::<Vec<_>>(),
        }
    } else {
        catalog.source_for_query(sql)?.clone()
    };

    let engine = SqlLikeQueryEngine.with_case_sensitive_strings(case_sensitive_strings);
    let mut execution = engine
        .execute_with_schema_and_resolver(&primary_source, sql, |table_ref| {
            catalog
                .resolved_table_data(table_ref)
                .map_err(QueryExecutionError::TableResolution)
        })
        .map_err(|err| err.to_string())?;

    if let Some(output_path) = output.as_deref() {
        match detect_export_format(output_path, lang)? {
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

fn run_session(
    path: &str,
    header: bool,
    case_sensitive_strings: bool,
    lang: CliLanguage,
) -> Result<(), String> {
    let mut catalog = SessionCatalog::new(path, lang)?;
    let engine = SqlLikeQueryEngine.with_case_sensitive_strings(case_sensitive_strings);

    println!("{}", tr(lang, "session_mode"));
    println!("{}", tr(lang, "session_intro"));

    if io::stdin().is_terminal() && io::stdout().is_terminal() {
        let mut editor = DefaultEditor::new()
            .map_err(|err| format!("{}: {err}", tr(lang, "failed_init_editor")))?;

        loop {
            match editor.readline("query-sheets> ") {
                Ok(line) => {
                    let input = line.trim();
                    if input.is_empty() {
                        continue;
                    }

                    let _ = editor.add_history_entry(input);

                    if !process_session_input(input, header, &engine, &mut catalog, lang) {
                        break;
                    }
                }
                Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
                Err(err) => return Err(format!("{}: {err}", tr(lang, "failed_read_interactive"))),
            }
        }
    } else {
        let stdin = io::stdin();
        loop {
            print!("query-sheets> ");
            io::stdout()
                .flush()
                .map_err(|err| format!("{}: {err}", tr(lang, "failed_flush_prompt")))?;

            let mut line = String::new();
            let bytes_read = stdin
                .read_line(&mut line)
                .map_err(|err| format!("{}: {err}", tr(lang, "failed_read_input")))?;

            if bytes_read == 0 {
                break;
            }

            if !process_session_input(line.trim(), header, &engine, &mut catalog, lang) {
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
    lang: CliLanguage,
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
        println!("{}", tr(lang, "help_commands"));
        println!("{}", tr(lang, "help_help"));
        println!("{}", tr(lang, "help_cache"));
        println!("{}", tr(lang, "help_clear"));
        println!("{}", tr(lang, "help_exit"));
        return true;
    }

    if input.eq_ignore_ascii_case(".cache") {
        println!(
            "{}",
            trf(lang, "cached_tables", &[catalog.cache_len().to_string()])
        );
        return true;
    }

    if matches!(input.to_ascii_lowercase().as_str(), ".clear" | "clear") {
        if let Err(err) = clear_console() {
            eprintln!("{}: {err}", tr(lang, "error_prefix"));
        }
        return true;
    }

    let sql = input.trim_end_matches(';').trim();

    let primary_source = match catalog.source_for_query(sql) {
        Ok(source) => source.clone(),
        Err(err) => {
            eprintln!("{}: {err}", tr(lang, "error_prefix"));
            return true;
        }
    };

    let mut execution = match engine.execute_with_schema_and_resolver(&primary_source, sql, |table_ref| {
        catalog
            .resolved_table_data(table_ref)
            .map_err(QueryExecutionError::TableResolution)
    }) {
        Ok(execution) => execution,
        Err(err) => {
            eprintln!("{}: {err}", tr(lang, "error_prefix"));
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
    lang: CliLanguage,
}

impl SessionCatalog {
    fn new(path: &str, lang: CliLanguage) -> Result<Self, String> {
        let resolved = PathBuf::from(path);

        if resolved.is_file() {
            let alias = file_alias(&resolved)?;
            return Ok(Self {
                mode: SessionCatalogMode::SingleFile {
                    file_path: resolved,
                    alias,
                },
                cache: HashMap::new(),
                lang,
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
                lang,
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
            .ok_or_else(|| tr(self.lang, "query_missing_table").to_string())?;

        self.source_for_table_reference(&table_ref)
    }

    fn resolved_table_data(&mut self, table_ref: &TableReference) -> Result<ResolvedTableData, String> {
        let source = self.source_for_table_reference(table_ref)?;
        Ok(ResolvedTableData {
            schema: source.schema.clone(),
            rows: source.rows.clone(),
        })
    }

    fn source_for_table_reference(
        &mut self,
        table_ref: &TableReference,
    ) -> Result<&CachedTableSource, String> {
        let (file_alias, file_path, table_name) = self.resolve_table_reference(table_ref)?;
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
                    tr(self.lang, "folder_mode_from_hint").to_string()
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

fn detect_export_format(
    output_path: &str,
    lang: CliLanguage,
) -> Result<DetectedExportFormat, String> {
    let extension = Path::new(output_path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase())
        .ok_or_else(|| trf(lang, "export_format_not_detected", &[output_path.to_string()]))?;

    match extension.as_str() {
        "csv" => Ok(DetectedExportFormat::Csv),
        "json" => Ok(DetectedExportFormat::Json),
        "jsonl" => Ok(DetectedExportFormat::Jsonl),
        _ => Err(trf(lang, "export_extension_unsupported", &[extension])),
    }
}

fn tr(lang: CliLanguage, key: &str) -> &'static str {
    match (lang, key) {
        (CliLanguage::En, "error_prefix") => "error",
        (CliLanguage::En, "session_mode") => "query-sheets session mode",
        (CliLanguage::En, "session_intro") => {
            "Type SQL and press Enter. Use .exit to quit, .help for help, and .clear to clear."
        }
        (CliLanguage::En, "failed_init_editor") => "failed to initialize interactive editor",
        (CliLanguage::En, "failed_read_interactive") => "failed to read interactive input",
        (CliLanguage::En, "failed_flush_prompt") => "failed to flush prompt",
        (CliLanguage::En, "failed_read_input") => "failed to read input",
        (CliLanguage::En, "help_commands") => "Commands:",
        (CliLanguage::En, "help_help") => "  .help  - show this help",
        (CliLanguage::En, "help_cache") => "  .cache - show number of cached tables",
        (CliLanguage::En, "help_clear") => "  .clear - clear the console",
        (CliLanguage::En, "help_exit") => "  .exit  - leave session mode",
        (CliLanguage::En, "query_missing_table") => "query must include a table in FROM",
        (CliLanguage::En, "folder_mode_from_hint") => {
            "in folder mode use FROM <file_alias>.<worksheet>"
        }

        (CliLanguage::PtBr, "error_prefix") => "erro",
        (CliLanguage::PtBr, "session_mode") => "modo sessao do query-sheets",
        (CliLanguage::PtBr, "session_intro") => {
            "Digite SQL e pressione Enter. Use .exit para sair, .help para ajuda e .clear para limpar."
        }
        (CliLanguage::PtBr, "failed_init_editor") => {
            "falha ao inicializar o editor interativo"
        }
        (CliLanguage::PtBr, "failed_read_interactive") => {
            "falha ao ler entrada interativa"
        }
        (CliLanguage::PtBr, "failed_flush_prompt") => "falha ao exibir o prompt",
        (CliLanguage::PtBr, "failed_read_input") => "falha ao ler entrada",
        (CliLanguage::PtBr, "help_commands") => "Comandos:",
        (CliLanguage::PtBr, "help_help") => "  .help  - mostra esta ajuda",
        (CliLanguage::PtBr, "help_cache") => "  .cache - mostra quantidade de tabelas no cache",
        (CliLanguage::PtBr, "help_clear") => "  .clear - limpa o console",
        (CliLanguage::PtBr, "help_exit") => "  .exit  - encerra o modo sessao",
        (CliLanguage::PtBr, "query_missing_table") => "a query deve incluir uma tabela no FROM",
        (CliLanguage::PtBr, "folder_mode_from_hint") => {
            "no modo pasta use FROM <arquivo>.<worksheet>"
        }
        _ => "",
    }
}

fn trf(lang: CliLanguage, key: &str, args: &[String]) -> String {
    match (lang, key) {
        (CliLanguage::En, "cached_tables") => format!("cached tables: {}", args[0]),
        (CliLanguage::En, "export_format_not_detected") => format!(
            "could not detect export format from '{}'. Use an output file ending with .csv, .json, or .jsonl",
            args[0]
        ),
        (CliLanguage::En, "export_extension_unsupported") => format!(
            "unsupported output extension '.{}'. Supported extensions: .csv, .json, .jsonl",
            args[0]
        ),

        (CliLanguage::PtBr, "cached_tables") => format!("tabelas em cache: {}", args[0]),
        (CliLanguage::PtBr, "export_format_not_detected") => format!(
            "nao foi possivel detectar o formato de exportacao em '{}'. Use um arquivo de saida com extensao .csv, .json ou .jsonl",
            args[0]
        ),
        (CliLanguage::PtBr, "export_extension_unsupported") => format!(
            "extensao de saida '.{}' nao suportada. Extensoes suportadas: .csv, .json, .jsonl",
            args[0]
        ),
        _ => String::new(),
    }
}
