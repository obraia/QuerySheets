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

export type StatusMessage = {
  message: string;
  isError: boolean;
};