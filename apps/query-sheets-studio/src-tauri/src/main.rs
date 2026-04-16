#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use calamine::{Reader, open_workbook_auto};
use query_sheets_adapters::create_excel_source;
use query_sheets_core::{DataSource, Row, Schema};
use query_sheets_query::{
    QueryEngine, QueryError, ResolvedTableData, SqlLikeQueryEngine, TableReference,
    extract_table_reference,
};
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

#[derive(Debug, Clone, Serialize)]
struct WorkbookInfo {
    alias: String,
    file_name: String,
    file_path: String,
    sheets: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkspaceOverview {
    root_path: String,
    files: Vec<WorkbookInfo>,
    cached_tables: usize,
}

#[derive(Debug, Clone, Serialize)]
struct QueryResultPayload {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    displayed_rows: usize,
    elapsed_ms: u128,
    truncated: bool,
}

#[derive(Debug, Clone)]
struct WorkbookEntry {
    alias: String,
    file_path: PathBuf,
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

#[derive(Debug, Clone)]
struct FolderCatalog {
    files_by_alias: HashMap<String, WorkbookEntry>,
    cache: HashMap<String, CachedTableSource>,
}

impl FolderCatalog {
    fn new(folder_path: &Path) -> Result<(Self, Vec<WorkbookInfo>), String> {
        if !folder_path.is_dir() {
            return Err(format!(
                "path '{}' is not a folder",
                folder_path.display()
            ));
        }

        let entries = std::fs::read_dir(folder_path)
            .map_err(|err| format!("failed to read directory '{}': {err}", folder_path.display()))?;

        let mut files_by_alias: HashMap<String, WorkbookEntry> = HashMap::new();
        let mut files = Vec::new();

        for entry in entries {
            let entry = entry.map_err(|err| format!("failed to read directory entry: {err}"))?;
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

            let sheets = workbook_sheets(&file_path)?;
            let file_name = file_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("unknown")
                .to_string();

            files.push(WorkbookInfo {
                alias: alias.clone(),
                file_name,
                file_path: file_path.display().to_string(),
                sheets,
            });

            files_by_alias.insert(alias_key, WorkbookEntry { alias, file_path });
        }

        if files.is_empty() {
            return Err(format!(
                "directory '{}' has no supported spreadsheet files",
                folder_path.display()
            ));
        }

        files.sort_by(|a, b| a.alias.cmp(&b.alias));

        Ok((
            Self {
                files_by_alias,
                cache: HashMap::new(),
            },
            files,
        ))
    }

    fn cache_len(&self) -> usize {
        self.cache.len()
    }

    fn source_for_query(&mut self, sql: &str) -> Result<&CachedTableSource, String> {
        let table_ref = extract_table_reference(sql)
            .map_err(|err| err.to_string())?
            .ok_or_else(|| "query must include a table in FROM".to_string())?;

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
        let (file_alias_value, file_path, sheet_name) = self.resolve_table_reference(table_ref)?;
        let cache_key = build_cache_key(&file_alias_value, &sheet_name);

        if !self.cache.contains_key(&cache_key) {
            let file_path_str = file_path
                .to_str()
                .ok_or_else(|| format!("invalid file path '{}'", file_path.display()))?;

            let source = create_excel_source(file_path_str, Some(&sheet_name))
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
        let schema = table_ref.schema.as_deref().ok_or_else(|| {
            "in folder mode use FROM <arquivo>.<worksheet> (example: vendas.sheet1)".to_string()
        })?;

        let alias_key = schema.to_ascii_lowercase();
        let file_entry = self.files_by_alias.get(&alias_key).ok_or_else(|| {
            let mut aliases = self
                .files_by_alias
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

#[derive(Debug)]
struct WorkspaceState {
    root_path: PathBuf,
    overview: WorkspaceOverview,
    catalog: FolderCatalog,
}

#[derive(Debug, Default)]
struct AppState {
    workspace: Option<WorkspaceState>,
}

#[tauri::command]
fn set_workspace_folder(
    folder_path: String,
    state: tauri::State<'_, Mutex<AppState>>,
) -> Result<WorkspaceOverview, String> {
    let root_path = PathBuf::from(folder_path);
    let (catalog, files) = FolderCatalog::new(&root_path)?;

    let overview = WorkspaceOverview {
        root_path: root_path.display().to_string(),
        files,
        cached_tables: 0,
    };

    let mut guard = state
        .lock()
        .map_err(|_| "internal state lock poisoned".to_string())?;

    guard.workspace = Some(WorkspaceState {
        root_path,
        overview: overview.clone(),
        catalog,
    });

    Ok(overview)
}

#[tauri::command]
fn refresh_workspace_overview(
    state: tauri::State<'_, Mutex<AppState>>,
) -> Result<WorkspaceOverview, String> {
    let mut guard = state
        .lock()
        .map_err(|_| "internal state lock poisoned".to_string())?;

    let workspace = guard
        .workspace
        .as_mut()
        .ok_or_else(|| "no folder opened".to_string())?;

    let (catalog, files) = FolderCatalog::new(&workspace.root_path)?;
    workspace.catalog = catalog;
    workspace.overview.files = files;
    workspace.overview.cached_tables = 0;

    Ok(workspace.overview.clone())
}

#[tauri::command]
fn execute_sql(
    sql: String,
    case_sensitive_strings: Option<bool>,
    max_rows: Option<usize>,
    state: tauri::State<'_, Mutex<AppState>>,
) -> Result<QueryResultPayload, String> {
    let started = Instant::now();

    let mut guard = state
        .lock()
        .map_err(|_| "internal state lock poisoned".to_string())?;

    let workspace = guard
        .workspace
        .as_mut()
        .ok_or_else(|| "no folder opened".to_string())?;

    let source = workspace.catalog.source_for_query(&sql)?.clone();

    let engine = SqlLikeQueryEngine.with_case_sensitive_strings(
        case_sensitive_strings.unwrap_or(false),
    );

    let limit = max_rows.unwrap_or(2000);
    let (columns, rows, truncated) = {
        let execution = engine
            .execute_with_schema_and_resolver(&source, &sql, |table_ref| {
                workspace
                    .catalog
                    .resolved_table_data(table_ref)
                    .map_err(QueryError::TableResolution)
            })
            .map_err(|err| err.to_string())?;

        let columns = execution
            .schema
            .columns
            .into_iter()
            .map(|column| column.name)
            .collect::<Vec<_>>();

        let mut rows = execution
            .rows
            .take(limit + 1)
            .map(|row| {
                row.values
                    .into_iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let truncated = rows.len() > limit;
        if truncated {
            let _ = rows.pop();
        }

        (columns, rows, truncated)
    };

    let payload = QueryResultPayload {
        columns,
        displayed_rows: rows.len(),
        rows,
        elapsed_ms: started.elapsed().as_millis(),
        truncated,
    };

    workspace.overview.cached_tables = workspace.catalog.cache_len();

    Ok(payload)
}

fn workbook_sheets(path: &Path) -> Result<Vec<String>, String> {
    let workbook = open_workbook_auto(path)
        .map_err(|err| format!("failed to open workbook '{}': {err}", path.display()))?;

    let names = workbook
        .sheet_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();

    if names.is_empty() {
        return Err(format!("workbook '{}' has no worksheets", path.display()));
    }

    Ok(names)
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

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(Mutex::new(AppState::default()))
        .invoke_handler(tauri::generate_handler![
            set_workspace_folder,
            refresh_workspace_overview,
            execute_sql
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
