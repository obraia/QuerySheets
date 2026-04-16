import { useMemo, useState } from "react";
import { open } from "@tauri-apps/plugin-dialog";
import { ExplorerPanel } from "./components/ExplorerPanel";
import { ResultsPanel } from "./components/ResultsPanel";
import { SqlEditorPanel } from "./components/SqlEditorPanel";
import { StatusBar } from "./components/StatusBar";
import { TitleBar } from "./components/TitleBar";
import {
  executeSql,
  refreshWorkspaceOverview,
  setWorkspaceFolder
} from "./services/queryStudioApi";
import type { QueryResult, StatusMessage, WorkspaceOverview } from "./types/query";

const defaultSql = [
  "SELECT c.CustomerName, o.Amount",
  "FROM exemplo.Customers c",
  "LEFT JOIN exemplo.Orders o ON c.CustomerId = o.CustomerId",
  "LIMIT 100"
].join("\n");

export function App(): JSX.Element {
  const [workspace, setWorkspace] = useState<WorkspaceOverview | null>(null);
  const [sql, setSql] = useState(defaultSql);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [resultMeta, setResultMeta] = useState("No query executed");
  const [error, setError] = useState<string | null>(null);
  const [isRunningQuery, setIsRunningQuery] = useState(false);
  const [status, setStatus] = useState<StatusMessage>({
    message: "Ready",
    isError: false
  });

  const folderPath = workspace?.root_path ?? "Open a folder with spreadsheets";

  const isBusy = useMemo(() => isRunningQuery, [isRunningQuery]);

  async function handleOpenFolder(): Promise<void> {
    const folder = await open({ directory: true, multiple: false });

    if (!folder || typeof folder !== "string") {
      return;
    }

    try {
      const overview = await setWorkspaceFolder(folder);
      setWorkspace(overview);
      setError(null);
      setResult(null);
      setResultMeta("No query executed");

      if (overview.files.length > 0) {
        const first = overview.files[0];
        const firstSheet = first.sheets[0];
        setSql(`SELECT * FROM ${first.alias}.${firstSheet} LIMIT 100`);
      }

      setStatus({ message: "Folder opened", isError: false });
    } catch (err) {
      setStatus({ message: String(err), isError: true });
    }
  }

  async function handleRefreshWorkspace(): Promise<void> {
    if (!workspace) {
      setStatus({ message: "Open a folder before refreshing", isError: true });
      return;
    }

    try {
      const overview = await refreshWorkspaceOverview();
      setWorkspace(overview);
      setStatus({ message: "Workspace refreshed", isError: false });
    } catch (err) {
      setStatus({ message: String(err), isError: true });
    }
  }

  function handlePickSheet(alias: string, sheet: string): void {
    setSql(`SELECT * FROM ${alias}.${sheet} LIMIT 100`);
    setStatus({ message: `Loaded ${alias}.${sheet} into editor`, isError: false });
  }

  async function handleRunQuery(): Promise<void> {
    const trimmedSql = sql.trim();

    if (!trimmedSql) {
      setStatus({ message: "Type a query first", isError: true });
      return;
    }

    if (!workspace) {
      setStatus({ message: "Open a folder before querying", isError: true });
      return;
    }

    try {
      setIsRunningQuery(true);
      setError(null);
      setStatus({ message: "Running query...", isError: false });

      const queryResult = await executeSql(trimmedSql, false, 2000);
      setResult(queryResult);
      setResultMeta(
        `${queryResult.displayed_rows} row(s) in ${queryResult.elapsed_ms} ms${
          queryResult.truncated ? " (truncated)" : ""
        }`
      );
      setStatus({ message: "Query executed", isError: false });
    } catch (err) {
      setResult(null);
      setResultMeta("Query failed");
      setError(String(err));
      setStatus({ message: "Query failed", isError: true });
    } finally {
      setIsRunningQuery(false);
    }
  }

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_15%_20%,#eff6ff_0%,#f8fafc_35%,#fff7ed_100%)] text-slate-900">
      <div className="mx-auto grid max-w-[1500px] grid-cols-1 gap-4 px-4 py-4 lg:grid-cols-[320px_minmax(0,1fr)] lg:px-6 lg:py-6">
        <ExplorerPanel workspace={workspace} onPickSheet={handlePickSheet} />

        <section className="grid min-h-[calc(100vh-2rem)] grid-rows-[auto_auto_minmax(260px,1fr)_auto] gap-4 lg:min-h-[calc(100vh-3rem)]">
          <TitleBar
            folderPath={folderPath}
            onOpenFolder={handleOpenFolder}
            onRefresh={handleRefreshWorkspace}
            onRunQuery={handleRunQuery}
            isBusy={isBusy}
          />

          <SqlEditorPanel
            sql={sql}
            onSqlChange={setSql}
            onRunQuery={handleRunQuery}
            isRunning={isBusy}
          />

          <ResultsPanel result={result} resultMeta={resultMeta} error={error} />

          <StatusBar status={status} />
        </section>
      </div>
    </div>
  );
}