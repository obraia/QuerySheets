export type WorkbookInfo = {
  alias: string;
  file_name: string;
  file_path: string;
  sheets: string[];
  sheet_columns: Record<string, string[]>;
};

export type WorkspaceOverview = {
  root_path: string;
  files: WorkbookInfo[];
  cached_tables: number;
};

export type QueryResult = {
  columns: string[];
  rows: string[][];
  displayed_rows: number;
  elapsed_ms: number;
  truncated: boolean;
};

export type ExportFormat = "csv" | "json" | "jsonl";

export type ExportResult = {
  output_path: string;
  format: ExportFormat;
  exported_rows: number;
  elapsed_ms: number;
};

export type StatusMessage = {
  message: string;
  isError: boolean;
};

export type QueryHistoryEntry = {
  id: string;
  sql: string;
  workspace_root_path: string;
  created_at: string;
};
