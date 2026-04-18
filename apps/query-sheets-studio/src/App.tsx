import { lazy, Suspense, useCallback, useEffect, useMemo, useState } from "react";
import { open, save } from "@tauri-apps/plugin-dialog";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { ExplorerPanel } from "./components/ExplorerPanel.js";
import { ResultsPanel } from "./components/ResultsPanel.js";
import { StatusBar } from "./components/StatusBar.js";
import { TitleBar } from "./components/TitleBar.js";
import { useI18n } from "./i18n";
import {
  executeSql,
  exportSql,
  refreshWorkspaceOverview,
  setParallelEnabled,
  setWorkspaceFolder
} from "./services/queryStudioApi.js";
import type {
  ExportFormat,
  QueryHistoryEntry,
  QueryResult,
  StatusMessage,
  WorkspaceOverview
} from "./types/query.js";

const defaultSql = [
  "SELECT c.CustomerName, o.Amount",
  "FROM sheet_name.Customers c",
  "LEFT JOIN sheet_name.Orders o ON c.CustomerId = o.CustomerId",
  "LIMIT 100"
].join("\n");

const MIN_QUERY_LOADING_MS = 280;
const PAGE_SIZE_OPTIONS = [25, 50, 100, 250, 500];
const SPREADSHEET_EXTENSIONS = [".xlsx", ".xlsm", ".xls", ".xlsb", ".ods"];
const PARALLEL_PREF_KEY = "querysheets.parallelEnabled";
const QUERY_HISTORY_KEY = "querysheets.queryHistory";
const QUERY_HISTORY_LIMIT = 8;

function loadParallelPreference(): boolean {
  if (typeof window === "undefined") {
    return true;
  }

  const raw = window.localStorage.getItem(PARALLEL_PREF_KEY);
  if (raw === null) {
    return true;
  }

  return raw !== "false";
}

function loadQueryHistory(): QueryHistoryEntry[] {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    const raw = window.localStorage.getItem(QUERY_HISTORY_KEY);
    if (!raw) {
      return [];
    }

    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed.filter((entry): entry is QueryHistoryEntry => {
      return (
        typeof entry === "object" &&
        entry !== null &&
        typeof entry.id === "string" &&
        typeof entry.sql === "string" &&
        typeof entry.workspace_root_path === "string" &&
        typeof entry.created_at === "string"
      );
    });
  } catch {
    return [];
  }
}

function persistQueryHistory(history: QueryHistoryEntry[]): void {
  if (typeof window === "undefined") {
    return;
  }

  window.localStorage.setItem(QUERY_HISTORY_KEY, JSON.stringify(history));
}

function isSpreadsheetFilePath(path: string): boolean {
  const lowerPath = path.toLowerCase();
  return SPREADSHEET_EXTENSIONS.some((extension) => lowerPath.endsWith(extension));
}

function parentDirectory(path: string): string | null {
  const trimmed = path.replace(/[\\/]+$/, "");
  const separatorIndex = Math.max(trimmed.lastIndexOf("/"), trimmed.lastIndexOf("\\"));

  if (separatorIndex < 0) {
    return null;
  }

  if (separatorIndex === 0) {
    return trimmed.slice(0, 1);
  }

  return trimmed.slice(0, separatorIndex);
}

function resolveDroppedFolderPath(path: string): string | null {
  if (isSpreadsheetFilePath(path)) {
    return parentDirectory(path);
  }

  return path;
}

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
  const { t } = useI18n();
  const [workspace, setWorkspace] = useState<WorkspaceOverview | null>(null);
  const [sql, setSql] = useState(defaultSql);
  const [queryHistory, setQueryHistory] = useState<QueryHistoryEntry[]>(loadQueryHistory);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [resultMeta, setResultMeta] = useState(() => t("status.noQuery"));
  const [error, setError] = useState<string | null>(null);
  const [isRunningQuery, setIsRunningQuery] = useState(false);
  const [isExporting, setIsExporting] = useState(false);
  const [parallelEnabled, setParallelExecutionEnabled] = useState(loadParallelPreference);
  const [isDragOverWindow, setIsDragOverWindow] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);
  const [status, setStatus] = useState<StatusMessage>({
    message: t("status.ready"),
    isError: false
  });

  const folderPath = workspace?.root_path ?? t("status.openFolderBeforeQuery");

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

  const rememberQueryExecution = useCallback((sqlText: string, workspaceRootPath: string): void => {
    const trimmedSql = sqlText.trim();
    if (!trimmedSql || !workspaceRootPath.trim()) {
      return;
    }

    setQueryHistory((current) => {
      const nextEntry: QueryHistoryEntry = {
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
        sql: trimmedSql,
        workspace_root_path: workspaceRootPath,
        created_at: new Date().toISOString()
      };

      const deduped = current.filter((entry) => {
        return !(
          entry.sql.trim() === trimmedSql &&
          entry.workspace_root_path === workspaceRootPath
        );
      });

      const nextHistory = [nextEntry, ...deduped].slice(0, QUERY_HISTORY_LIMIT);
      persistQueryHistory(nextHistory);
      return nextHistory;
    });
  }, []);

  const openWorkspaceFolder = useCallback(async (
    folderPath: string,
    options?: { initialSql?: string; statusMessage?: string }
  ): Promise<void> => {
    try {
      const overview = await setWorkspaceFolder(folderPath);
      setWorkspace(overview);
      setError(null);
      setResult(null);
      setCurrentPage(1);
      setResultMeta(t("status.noQuery"));

      if (typeof options?.initialSql === "string") {
        setSql(options.initialSql);
      } else if (overview.files.length > 0) {
        const first = overview.files[0];
        const firstSheet = first.sheets[0];
        setSql(`SELECT * FROM ${first.alias}.${firstSheet} LIMIT 100`);
      }

      setStatus({ message: options?.statusMessage ?? t("status.folderOpened"), isError: false });
    } catch (err) {
      setStatus({ message: String(err), isError: true });
    }
  }, [t]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem(PARALLEL_PREF_KEY, parallelEnabled ? "true" : "false");
  }, [parallelEnabled]);

  useEffect(() => {
    void setParallelEnabled(parallelEnabled).catch((err) => {
      setStatus({
        message: t("status.parallelUnavailable", { error: String(err) }),
        isError: true
      });
    });
  }, [parallelEnabled, t]);

  useEffect(() => {
    let unlisten: (() => void) | null = null;
    let disposed = false;

    void getCurrentWindow()
      .onDragDropEvent((event) => {
        const dragType = event.payload.type;

        if (dragType === "enter" || dragType === "over") {
          setIsDragOverWindow(true);
          return;
        }

        if (dragType === "leave") {
          setIsDragOverWindow(false);
          return;
        }

        if (dragType === "drop") {
          setIsDragOverWindow(false);
          const droppedPath = event.payload.paths[0];

          if (!droppedPath) {
            setStatus({ message: t("status.dragNoFolder"), isError: true });
            return;
          }

          const resolvedFolderPath = resolveDroppedFolderPath(droppedPath);
          if (!resolvedFolderPath) {
            setStatus({ message: t("status.dragResolveFailed"), isError: true });
            return;
          }

          void openWorkspaceFolder(resolvedFolderPath);
        }
      })
      .then((fn) => {
        if (disposed) {
          fn();
          return;
        }

        unlisten = fn;
      })
      .catch((err) => {
        setStatus({ message: t("status.dragUnavailable", { error: String(err) }), isError: true });
      });

    return () => {
      disposed = true;
      if (unlisten) {
        unlisten();
      }
    };
  }, [openWorkspaceFolder, t]);

  async function handleOpenFolder(): Promise<void> {
    const folder = await open({ directory: true, multiple: false });

    if (!folder || typeof folder !== "string") {
      return;
    }

    await openWorkspaceFolder(folder);
  }

  async function handleRefreshWorkspace(): Promise<void> {
    if (!workspace) {
      setStatus({ message: t("status.openFolderBeforeRefresh"), isError: true });
      return;
    }

    try {
      const overview = await refreshWorkspaceOverview();
      setWorkspace(overview);
      setStatus({ message: t("status.workspaceRefreshed"), isError: false });
    } catch (err) {
      setStatus({ message: String(err), isError: true });
    }
  }

  function handlePickSheet(alias: string, sheet: string): void {
    setSql(`SELECT * FROM ${alias}.${sheet} LIMIT 100`);
    setStatus({ message: t("status.loadedSheet", { table: `${alias}.${sheet}` }), isError: false });
  }

  async function handleRunQuery(): Promise<void> {
    const trimmedSql = sql.trim();

    if (!trimmedSql) {
      setStatus({ message: t("status.typeQueryFirst"), isError: true });
      return;
    }

    if (!workspace) {
      setStatus({ message: t("status.openFolderBeforeQuery"), isError: true });
      return;
    }

    const loadingStart = performance.now();

    try {
      setIsRunningQuery(true);
      setError(null);
      setStatus({ message: t("status.runningQuery"), isError: false });

      // Give React one paint cycle so the loading indicator is visible
      // before the IPC/query work starts.
      await waitForUiPaint();

      const queryResult = await executeSql(trimmedSql, false);
      setResult(queryResult);
      rememberQueryExecution(trimmedSql, workspace.root_path);
      setCurrentPage(1);
      setResultMeta(
        t("meta.rows", {
          rows: queryResult.displayed_rows,
          elapsed: queryResult.elapsed_ms,
          suffix: queryResult.truncated ? t("meta.truncated") : ""
        })
      );
      setStatus({ message: t("status.queryExecuted"), isError: false });
    } catch (err) {
      setResult(null);
      setResultMeta(t("status.queryFailed"));
      setError(String(err));
      setStatus({ message: t("status.queryFailed"), isError: true });
    } finally {
      const elapsed = performance.now() - loadingStart;
      if (elapsed < MIN_QUERY_LOADING_MS) {
        await sleep(MIN_QUERY_LOADING_MS - elapsed);
      }

      setIsRunningQuery(false);
    }
  }

  async function handleSelectQueryHistory(entry: QueryHistoryEntry): Promise<void> {
    await openWorkspaceFolder(entry.workspace_root_path, {
      initialSql: entry.sql,
      statusMessage: t("status.historyLoaded")
    });
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
      setStatus({ message: t("status.typeQueryFirst"), isError: true });
      return;
    }

    if (!workspace) {
      setStatus({ message: t("status.openFolderBeforeExport"), isError: true });
      return;
    }

    try {
      setIsExporting(true);
      setStatus({
        message: t("status.exporting", { format: format.toUpperCase() }),
        isError: false
      });

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
        setStatus({ message: t("status.exportCanceled"), isError: false });
        return;
      }

      const exportResult = await exportSql(trimmedSql, selectedPath, format, false);
      setStatus({
        message: t("status.exported", {
          rows: exportResult.exported_rows,
          path: exportResult.output_path,
          elapsed: exportResult.elapsed_ms
        }),
        isError: false
      });
    } catch (err) {
      setStatus({ message: t("status.exportFailed", { error: String(err) }), isError: true });
    } finally {
      setIsExporting(false);
    }
  }

  function handleParallelToggle(enabled: boolean): void {
    setParallelExecutionEnabled(enabled);
    setStatus({
      message: enabled ? t("status.parallelEnabled") : t("status.parallelDisabled"),
      isError: false
    });
  }

  return (
    <div className="relative h-screen overflow-hidden bg-[radial-gradient(circle_at_15%_20%,#eff6ff_0%,#f8fafc_35%,#fff7ed_100%)] text-slate-900">
      <div className="mx-auto grid h-full max-w-[1500px] min-h-0 grid-cols-1 gap-4 px-4 py-4 lg:grid-cols-[320px_minmax(0,1fr)] lg:px-6 lg:py-6">
        <ExplorerPanel workspace={workspace} onPickSheet={handlePickSheet} />

        <section className="grid h-full min-h-0 grid-rows-[auto_auto_auto_minmax(0,1fr)] gap-4 overflow-hidden">
          <TitleBar
            folderPath={folderPath}
            onOpenFolder={handleOpenFolder}
            onRefresh={handleRefreshWorkspace}
            onRunQuery={handleRunQuery}
            onParallelToggle={handleParallelToggle}
            parallelEnabled={parallelEnabled}
            isBusy={isBusy}
            isRunning={isRunningQuery}
          />

          <Suspense
            fallback={
              <section className="rounded-2xl border border-slate-200/70 bg-white/90 shadow-[0_16px_40px_-26px_rgba(15,23,42,0.45)] backdrop-blur-md">
                <header className="flex items-center justify-between border-b border-slate-100 px-4 py-3 lg:px-5">
                  <p className="text-sm font-semibold text-slate-800">query.sql</p>
                  <p className="text-xs text-slate-500">{t("editor.loading")}</p>
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
              queryHistory={queryHistory}
              onSelectQueryHistory={handleSelectQueryHistory}
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

      {isDragOverWindow && (
        <div className="pointer-events-none absolute inset-4 z-50 flex items-center justify-center rounded-3xl border-2 border-dashed border-teal-400/80 bg-white/65 backdrop-blur-sm">
          <div className="rounded-xl border border-teal-200 bg-white px-5 py-3 text-sm font-semibold text-teal-700 shadow-sm">
            {t("dropzone.hint")}
          </div>
        </div>
      )}
    </div>
  );
}
