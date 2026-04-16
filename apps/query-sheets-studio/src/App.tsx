import { lazy, Suspense, useMemo, useState } from "react";
import { open, save } from "@tauri-apps/plugin-dialog";
import { ExplorerPanel } from "./components/ExplorerPanel.js";
import { ResultsPanel } from "./components/ResultsPanel.js";
import { StatusBar } from "./components/StatusBar.js";
import { TitleBar } from "./components/TitleBar.js";
import {
  executeSql,
  exportSql,
  refreshWorkspaceOverview,
  setWorkspaceFolder
} from "./services/queryStudioApi.js";
import type { ExportFormat, QueryResult, StatusMessage, WorkspaceOverview } from "./types/query.js";

const defaultSql = [
  "SELECT c.CustomerName, o.Amount",
  "FROM exemplo.Customers c",
  "LEFT JOIN exemplo.Orders o ON c.CustomerId = o.CustomerId",
  "LIMIT 100"
].join("\n");

const MIN_QUERY_LOADING_MS = 280;
const PAGE_SIZE_OPTIONS = [25, 50, 100, 250, 500];

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function waitForUiPaint(): Promise<void> {
  return new Promise((resolve) => {
    if (typeof window === "undefined" || typeof window.requestAnimationFrame !== "function") {
      setTimeout(resolve, 0);
      return;
    }

    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => resolve());
    });
  });
}

const SqlEditorPanel = lazy(async () => {
  const module = await import("./components/SqlEditorPanel.js");
  return { default: module.SqlEditorPanel };
});

export function App(): JSX.Element {
  const [workspace, setWorkspace] = useState<WorkspaceOverview | null>(null);
  const [sql, setSql] = useState(defaultSql);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [resultMeta, setResultMeta] = useState("No query executed");
  const [error, setError] = useState<string | null>(null);
  const [isRunningQuery, setIsRunningQuery] = useState(false);
  const [isExporting, setIsExporting] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);
  const [status, setStatus] = useState<StatusMessage>({
    message: "Ready",
    isError: false
  });

  const folderPath = workspace?.root_path ?? "Open a folder with spreadsheets";

  const isBusy = useMemo(() => isRunningQuery || isExporting, [isExporting, isRunningQuery]);
  const totalRows = result?.rows.length ?? 0;
  const totalPages = useMemo(() => {
    if (!result) {
      return 1;
    }

    return Math.max(1, Math.ceil(result.rows.length / pageSize));
  }, [pageSize, result]);
  const hasNextPage = currentPage < totalPages;
  const paginatedResult = useMemo<QueryResult | null>(() => {
    if (!result) {
      return null;
    }

    const from = (currentPage - 1) * pageSize;
    const to = from + pageSize;
    const pageRows = result.rows.slice(from, to);

    return {
      ...result,
      rows: pageRows,
      displayed_rows: pageRows.length,
      truncated: false
    };
  }, [currentPage, pageSize, result]);

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

    const loadingStart = performance.now();

    try {
      setIsRunningQuery(true);
      setError(null);
      setStatus({ message: "Running query...", isError: false });

      // Give React one paint cycle so the loading indicator is visible
      // before the IPC/query work starts.
      await waitForUiPaint();

      const queryResult = await executeSql(trimmedSql, false);
      setResult(queryResult);
      setCurrentPage(1);
      setResultMeta(
        `${queryResult.displayed_rows} total row(s) in ${queryResult.elapsed_ms} ms${
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
      const elapsed = performance.now() - loadingStart;
      if (elapsed < MIN_QUERY_LOADING_MS) {
        await sleep(MIN_QUERY_LOADING_MS - elapsed);
      }

      setIsRunningQuery(false);
    }
  }

  function handlePreviousPage(): void {
    if (currentPage <= 1) {
      return;
    }

    setCurrentPage((previous) => Math.max(1, previous - 1));
  }

  function handleNextPage(): void {
    if (!hasNextPage) {
      return;
    }

    setCurrentPage((previous) => Math.min(totalPages, previous + 1));
  }

  function handlePageSizeChange(nextPageSize: number): void {
    if (nextPageSize === pageSize) {
      return;
    }

    setPageSize(nextPageSize);
    setCurrentPage(1);
  }

  async function handleExportQuery(format: ExportFormat): Promise<void> {
    const trimmedSql = sql.trim();

    if (!trimmedSql) {
      setStatus({ message: "Type a query first", isError: true });
      return;
    }

    if (!workspace) {
      setStatus({ message: "Open a folder before exporting", isError: true });
      return;
    }

    try {
      setIsExporting(true);
      setStatus({ message: `Exporting ${format.toUpperCase()}...`, isError: false });

      const selectedPath = await save({
        defaultPath: `${workspace.root_path}/query-result.${format}`,
        filters: [
          {
            name: format.toUpperCase(),
            extensions: [format]
          }
        ]
      });

      if (!selectedPath || typeof selectedPath !== "string") {
        setStatus({ message: "Export canceled", isError: false });
        return;
      }

      const exportResult = await exportSql(trimmedSql, selectedPath, format, false);
      setStatus({
        message: `Exported ${exportResult.exported_rows} row(s) to ${exportResult.output_path} in ${exportResult.elapsed_ms} ms`,
        isError: false
      });
    } catch (err) {
      setStatus({ message: `Export failed: ${String(err)}`, isError: true });
    } finally {
      setIsExporting(false);
    }
  }

  return (
    <div className="h-screen overflow-hidden bg-[radial-gradient(circle_at_15%_20%,#eff6ff_0%,#f8fafc_35%,#fff7ed_100%)] text-slate-900">
      <div className="mx-auto grid h-full max-w-[1500px] min-h-0 grid-cols-1 gap-4 px-4 py-4 lg:grid-cols-[320px_minmax(0,1fr)] lg:px-6 lg:py-6">
        <ExplorerPanel workspace={workspace} onPickSheet={handlePickSheet} />

        <section className="grid h-full min-h-0 grid-rows-[auto_auto_auto_minmax(0,1fr)] gap-4 overflow-hidden">
          <TitleBar
            folderPath={folderPath}
            onOpenFolder={handleOpenFolder}
            onRefresh={handleRefreshWorkspace}
            onRunQuery={handleRunQuery}
            isBusy={isBusy}
            isRunning={isRunningQuery}
          />

          <Suspense
            fallback={
              <section className="rounded-2xl border border-slate-200/70 bg-white/90 shadow-[0_16px_40px_-26px_rgba(15,23,42,0.45)] backdrop-blur-md">
                <header className="flex items-center justify-between border-b border-slate-100 px-4 py-3 lg:px-5">
                  <p className="text-sm font-semibold text-slate-800">query.sql</p>
                  <p className="text-xs text-slate-500">Loading editor...</p>
                </header>
                <div className="min-h-[210px] w-full bg-slate-950/95" />
              </section>
            }
          >
            <SqlEditorPanel
              sql={sql}
              onSqlChange={setSql}
              onRunQuery={handleRunQuery}
              onExportQuery={handleExportQuery}
              isRunning={isRunningQuery}
              isExporting={isExporting}
              workspace={workspace}
            />
          </Suspense>

          <StatusBar status={status} />

          <ResultsPanel
            result={paginatedResult}
            resultMeta={resultMeta}
            error={error}
            isLoading={isRunningQuery}
            currentPage={currentPage}
            pageSize={pageSize}
            pageSizeOptions={PAGE_SIZE_OPTIONS}
            hasNextPage={hasNextPage}
            totalRows={totalRows}
            onPreviousPage={handlePreviousPage}
            onNextPage={handleNextPage}
            onPageSizeChange={handlePageSizeChange}
          />
        </section>
      </div>
    </div>
  );
}