import { invoke } from "@tauri-apps/api/core";
import type { ExportFormat, ExportResult, QueryResult, WorkspaceOverview } from "../types/query";

export async function setWorkspaceFolder(folderPath: string): Promise<WorkspaceOverview> {
  return invoke<WorkspaceOverview>("set_workspace_folder", { folderPath });
}

export async function refreshWorkspaceOverview(): Promise<WorkspaceOverview> {
  return invoke<WorkspaceOverview>("refresh_workspace_overview");
}

export async function executeSql(
  sql: string,
  caseSensitiveStrings: boolean,
  maxRows: number
): Promise<QueryResult> {
  return invoke<QueryResult>("execute_sql", {
    sql,
    caseSensitiveStrings,
    maxRows
  });
}

export async function exportSql(
  sql: string,
  outputPath: string,
  format: ExportFormat,
  caseSensitiveStrings: boolean
): Promise<ExportResult> {
  const normalizedPath = outputPath.toLowerCase().endsWith(`.${format}`)
    ? outputPath
    : `${outputPath}.${format}`;

  return invoke<ExportResult>("export_sql", {
    sql,
    outputPath: normalizedPath,
    caseSensitiveStrings
  });
}