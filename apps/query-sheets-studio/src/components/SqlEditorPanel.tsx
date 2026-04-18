import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import CodeMirror from "@uiw/react-codemirror";
import {
  autocompletion,
  startCompletion,
  type Completion,
  type CompletionContext,
  type CompletionResult
} from "@codemirror/autocomplete";
import { sql as sqlLanguage } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import { keymap } from "@codemirror/view";
import { ChevronDown, Download, History, X } from "lucide-react";
import type { ExportFormat, QueryHistoryEntry, WorkspaceOverview } from "../types/query";
import { useI18n } from "../i18n";

type SqlSuggestionContext = "select" | "fromJoin" | "filter" | "generic";

type AliasBinding = {
  alias: string;
  tableName: string;
  columns: string[];
};

const SQL_KEYWORDS = [
  "SELECT",
  "FROM",
  "WHERE",
  "JOIN",
  "LEFT JOIN",
  "RIGHT JOIN",
  "INNER JOIN",
  "ON",
  "GROUP BY",
  "ORDER BY",
  "HAVING",
  "LIMIT",
  "OFFSET",
  "AS",
  "ASC",
  "DESC",
  "NULLS FIRST",
  "NULLS LAST",
  "AND",
  "OR",
  "NOT",
  "LIKE",
  "NOT LIKE",
  "COUNT",
  "SUM",
  "AVG",
  "MIN",
  "MAX",
  "STDDEV",
  "CAST"
];

const EXPORT_OPTIONS: Array<{ format: ExportFormat; label: string; detail: string }> = [
  { format: "csv", label: "CSV", detail: ".csv" },
  { format: "json", label: "JSON", detail: ".json" },
  { format: "jsonl", label: "JSONL", detail: ".jsonl" }
];

function compactLocations(locations: string[]): string {
  if (locations.length <= 2) {
    return locations.join(", ");
  }

  return `${locations.slice(0, 2).join(", ")} (+${locations.length - 2})`;
}

function detectSuggestionContext(source: string): SqlSuggestionContext {
  const upper = source.toUpperCase();
  const keywordRegex = /\b(SELECT|FROM|JOIN|WHERE|ON|HAVING|GROUP\s+BY|ORDER\s+BY)\b/g;
  let lastKeyword: string | null = null;
  let match: RegExpExecArray | null;

  while (true) {
    match = keywordRegex.exec(upper);
    if (!match) {
      break;
    }
    lastKeyword = match[1];
  }

  if (!lastKeyword) {
    return "generic";
  }

  if (lastKeyword === "SELECT") {
    return "select";
  }

  if (lastKeyword === "FROM" || lastKeyword === "JOIN") {
    return "fromJoin";
  }

  if (lastKeyword === "WHERE" || lastKeyword === "ON" || lastKeyword === "HAVING") {
    return "filter";
  }

  return "generic";
}

function scoreCompletionByContext(type: Completion["type"], context: SqlSuggestionContext): number {
  switch (context) {
    case "select":
      if (type === "property") return 140;
      if (type === "variable") return 80;
      return 30;
    case "fromJoin":
      if (type === "variable") return 150;
      if (type === "namespace") return 110;
      return 20;
    case "filter":
      if (type === "property") return 130;
      if (type === "keyword") return 60;
      return 30;
    default:
      if (type === "keyword") return 70;
      if (type === "variable") return 65;
      if (type === "property") return 60;
      return 40;
  }
}

function extractAliasBindings(
  source: string,
  tableColumnsByName: Record<string, string[]>
): AliasBinding[] {
  const regex = /\b(?:FROM|JOIN)\s+([A-Za-z_][\w$]*\.[A-Za-z_][\w$]*)(?:\s+(?:AS\s+)?([A-Za-z_][\w$]*))?/gi;
  const aliases = new Map<string, AliasBinding>();
  let match: RegExpExecArray | null;

  while (true) {
    match = regex.exec(source);
    if (!match) {
      break;
    }

    const tableName = match[1];
    const aliasCandidate = match[2];

    if (!aliasCandidate) {
      continue;
    }

    const alias = aliasCandidate.trim();
    if (!alias) {
      continue;
    }

    if (SQL_KEYWORDS.includes(alias.toUpperCase())) {
      continue;
    }

    const tableKey = tableName.toLocaleLowerCase();
    const columns = tableColumnsByName[tableKey] ?? [];

    aliases.set(alias.toLocaleLowerCase(), {
      alias,
      tableName,
      columns
    });
  }

  return [...aliases.values()];
}

function buildAliasCompletions(
  source: string,
  tableColumnsByName: Record<string, string[]>
): Completion[] {
  const aliasBindings = extractAliasBindings(source, tableColumnsByName);
  const completions: Completion[] = [];

  for (const binding of aliasBindings) {
    completions.push({
      label: binding.alias,
      type: "variable",
      detail: `alias for ${binding.tableName}`
    });

    completions.push({
      label: `${binding.alias}.`,
      type: "variable",
      detail: "alias prefix"
    });

    for (const column of binding.columns) {
      completions.push({
        label: `${binding.alias}.${column}`,
        type: "property",
        detail: binding.tableName
      });
    }
  }

  return completions;
}

function dedupeCompletions(options: Completion[]): Completion[] {
  const seen = new Set<string>();
  const result: Completion[] = [];

  for (const option of options) {
    const key = `${option.type ?? "unknown"}:${option.label}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    result.push(option);
  }

  return result;
}

type SqlEditorPanelProps = {
  sql: string;
  onSqlChange: (value: string) => void;
  onRunQuery: () => Promise<void>;
  onExportQuery: (format: ExportFormat) => Promise<void>;
  queryHistory: QueryHistoryEntry[];
  onSelectQueryHistory: (entry: QueryHistoryEntry) => Promise<void>;
  isRunning: boolean;
  isExporting: boolean;
  workspace: WorkspaceOverview | null;
};

function formatHistoryTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return date.toLocaleString();
}

function summarizeSql(sql: string): string {
  const compact = sql.replace(/\s+/g, " ").trim();
  if (compact.length <= 96) {
    return compact;
  }

  return `${compact.slice(0, 93)}...`;
}

export function SqlEditorPanel({
  sql,
  onSqlChange,
  onRunQuery,
  onExportQuery,
  queryHistory,
  onSelectQueryHistory,
  isRunning,
  isExporting,
  workspace
}: SqlEditorPanelProps): JSX.Element {
  const { t } = useI18n();
  const [isExportMenuOpen, setIsExportMenuOpen] = useState(false);
  const [isHistoryModalOpen, setIsHistoryModalOpen] = useState(false);
  const exportMenuRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!isExportMenuOpen) {
      return;
    }

    const handleOutsideClick = (event: MouseEvent): void => {
      if (!exportMenuRef.current) {
        return;
      }

      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }

      if (!exportMenuRef.current.contains(target)) {
        setIsExportMenuOpen(false);
      }
    };

    window.addEventListener("mousedown", handleOutsideClick);
    return () => {
      window.removeEventListener("mousedown", handleOutsideClick);
    };
  }, [isExportMenuOpen]);

  useEffect(() => {
    if (!isExporting) {
      return;
    }

    setIsExportMenuOpen(false);
  }, [isExporting]);

  useEffect(() => {
    if (!isHistoryModalOpen) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent): void => {
      if (event.key === "Escape") {
        setIsHistoryModalOpen(false);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [isHistoryModalOpen]);

  const exportDisabled = isRunning || isExporting || !workspace;
  const historyDisabled = isRunning || isExporting;

  function handleExportSelectionChange(format: ExportFormat): void {
    setIsExportMenuOpen(false);
    void onExportQuery(format);
  }

  function handleSelectHistoryEntry(entry: QueryHistoryEntry): void {
    setIsHistoryModalOpen(false);
    void onSelectQueryHistory(entry);
  }

  const tableColumnsByName = useMemo(() => {
    const columnsByTable: Record<string, string[]> = {};

    for (const workbook of workspace?.files ?? []) {
      for (const sheet of workbook.sheets) {
        const tableName = `${workbook.alias}.${sheet}`;
        const columns = (workbook.sheet_columns?.[sheet] ?? [])
          .map((value) => value.trim())
          .filter((value) => value.length > 0);

        columnsByTable[tableName.toLocaleLowerCase()] = columns;
      }
    }

    return columnsByTable;
  }, [workspace]);

  const completionOptions = useMemo<Completion[]>(() => {
    const options: Completion[] = [];
    const columnOrigins = new Map<string, { label: string; locations: string[] }>();

    for (const workbook of workspace?.files ?? []) {
      options.push({
        label: workbook.alias,
        type: "namespace",
        detail: "file alias"
      });

      for (const sheet of workbook.sheets) {
        const tableName = `${workbook.alias}.${sheet}`;
        options.push({
          label: tableName,
          type: "variable",
          detail: "table"
        });

        for (const column of workbook.sheet_columns?.[sheet] ?? []) {
          const trimmed = column.trim();
          if (!trimmed) {
            continue;
          }

          const key = trimmed.toLocaleLowerCase();
          const location = `${workbook.alias}.${sheet}`;
          const existing = columnOrigins.get(key);

          if (existing) {
            if (!existing.locations.includes(location)) {
              existing.locations.push(location);
            }
          } else {
            columnOrigins.set(key, {
              label: trimmed,
              locations: [location]
            });
          }
        }
      }
    }

    for (const { label, locations } of columnOrigins.values()) {
      options.push({
        label,
        type: "property",
        detail: compactLocations(locations)
      });
    }

    for (const keyword of SQL_KEYWORDS) {
      options.push({
        label: keyword,
        type: "keyword"
      });
    }

    return options;
  }, [workspace]);

  const sqlSchema = useMemo(() => {
    const schema: Record<string, Record<string, readonly string[]>> = {};

    for (const workbook of workspace?.files ?? []) {
      const sheetMap: Record<string, readonly string[]> = {};
      for (const sheet of workbook.sheets) {
        const columns = (workbook.sheet_columns?.[sheet] ?? []).filter((name) => name.trim().length > 0);
        sheetMap[sheet] = columns;
      }
      schema[workbook.alias] = sheetMap;
    }

    return schema;
  }, [workspace]);

  const editorExtensions = useMemo(
    () => [
      sqlLanguage({
        schema: sqlSchema,
        upperCaseKeywords: true
      }),
      autocompletion({
        activateOnTyping: true,
        override: [
          (context: CompletionContext): CompletionResult | null => {
            const typed = context.matchBefore(/[A-Za-z_][\w$]*(?:\.[\w$]*)?/);

            if (!typed && !context.explicit) {
              return null;
            }

            const from = typed?.from ?? context.pos;
            const to = typed?.to ?? context.pos;
            const prefix = typed?.text.toLocaleLowerCase() ?? "";
            const sourceBeforeCursor = context.state.doc.sliceString(0, context.pos);
            const fullSource = context.state.doc.toString();
            const suggestionContext = detectSuggestionContext(
              sourceBeforeCursor
            );

            const aliasCompletions = buildAliasCompletions(
              fullSource,
              tableColumnsByName
            );

            const availableOptions = dedupeCompletions([
              ...aliasCompletions,
              ...completionOptions
            ]);

            const options = availableOptions
              .filter((option) =>
                prefix.length === 0
                  ? true
                  : option.label.toLocaleLowerCase().startsWith(prefix)
              )
              .map((option) => ({
                ...option,
                boost: scoreCompletionByContext(option.type, suggestionContext)
              }))
              .sort((a, b) => (b.boost ?? 0) - (a.boost ?? 0));

            if (options.length === 0) {
              return null;
            }

            return {
              from,
              to,
              options,
              validFor: /^[A-Za-z_][\w$]*(?:\.[\w$]*)?$/
            };
          }
        ]
      }),
      keymap.of([
        {
          key: "Mod-Space",
          run: startCompletion
        },
        {
          key: "Ctrl-Space",
          run: startCompletion
        },
        {
          key: "Alt-Space",
          run: startCompletion
        },
        {
          key: "Mod-Enter",
          run: () => {
            void onRunQuery();
            return true;
          }
        },
        {
          key: "Shift-Enter",
          run: () => {
            void onRunQuery();
            return true;
          }
        }
      ])
    ],
    [completionOptions, onRunQuery, sqlSchema, tableColumnsByName]
  );

  return (
    <>
      <section className="rounded-2xl border border-slate-200/70 bg-white/90 shadow-[0_16px_40px_-26px_rgba(15,23,42,0.45)] backdrop-blur-md">
        <header className="flex items-center justify-between border-b border-slate-100 px-4 py-3 lg:px-5">
          <p className="text-sm font-semibold text-slate-800">query.sql</p>
          <div className="relative flex items-center gap-3" ref={exportMenuRef}>
            <p className="text-xs text-slate-500">
              {isRunning
                ? t("editor.executing")
                : t("editor.shortcuts")}
            </p>

            <button
              type="button"
              aria-label={t("editor.history")}
              className="inline-flex items-center rounded-lg border border-transparent bg-white/70 p-1.5 text-slate-500 transition hover:border-slate-200 hover:text-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
              onClick={() => setIsHistoryModalOpen(true)}
              disabled={historyDisabled}
              title={t("editor.history")}
            >
              <History size={15} strokeWidth={2} />
            </button>

            <button
              type="button"
              aria-label="Export query result"
              aria-haspopup="menu"
              aria-expanded={isExportMenuOpen}
              className="inline-flex items-center gap-1 rounded-lg border border-slate-200 bg-white px-2 py-1.5 text-xs font-medium text-slate-600 transition hover:border-sky-300 hover:text-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
              onClick={() => setIsExportMenuOpen((open) => !open)}
              disabled={exportDisabled}
              title={!workspace ? t("editor.exportOpenFolder") : t("editor.export")}
            >
              <Download size={14} strokeWidth={2} />
              <ChevronDown size={14} strokeWidth={2} className={isExportMenuOpen ? "rotate-180" : ""} />
            </button>

            {isExportMenuOpen && (
              <div
                role="menu"
                aria-label="Export format options"
                className="absolute right-0 top-[calc(100%+8px)] z-20 w-48 rounded-xl border border-slate-200 bg-white p-2 shadow-lg"
              >
                <p className="mb-1 px-2 pt-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-500">
                  {t("editor.exportFormat")}
                </p>

                <div className="grid gap-1">
                  {EXPORT_OPTIONS.map((option) => (
                    <button
                      key={option.format}
                      type="button"
                      role="menuitem"
                      className="flex items-center justify-between rounded-lg px-2 py-1.5 text-sm text-slate-700 transition hover:bg-sky-50 hover:text-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
                      onClick={() => handleExportSelectionChange(option.format)}
                      disabled={isExporting}
                    >
                      <span className="font-medium">{option.label}</span>
                      <span className="text-xs text-slate-500">{option.detail}</span>
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>
        </header>

        <CodeMirror
          value={sql}
          height="240px"
          minHeight="210px"
          maxHeight="55vh"
          theme={oneDark}
          basicSetup={{
            lineNumbers: true,
            foldGutter: true,
            highlightActiveLine: true,
            autocompletion: false
          }}
          editable={!isRunning}
          extensions={editorExtensions}
          onChange={(value) => onSqlChange(value)}
          className="sql-editor"
        />
      </section>

      {renderHistoryModal(
        isHistoryModalOpen,
        t,
        queryHistory,
        historyDisabled,
        () => setIsHistoryModalOpen(false),
        handleSelectHistoryEntry
      )}
    </>
  );
}

function renderHistoryModal(
  isOpen: boolean,
  t: ReturnType<typeof useI18n>["t"],
  queryHistory: QueryHistoryEntry[],
  historyDisabled: boolean,
  onClose: () => void,
  onSelect: (entry: QueryHistoryEntry) => void
): JSX.Element | null {
  if (!isOpen || typeof document === "undefined") {
    return null;
  }

  return createPortal(
    <div
      className="fixed inset-0 z-[120] flex items-start justify-center bg-slate-950/30 px-4 py-10 backdrop-blur-[2px]"
      onClick={onClose}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-label={t("editor.history")}
        className="flex max-h-[70vh] w-full max-w-2xl flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-2xl"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-slate-100 px-4 py-3 lg:px-5">
          <div>
            <p className="text-sm font-semibold text-slate-800">{t("editor.history")}</p>
            <p className="mt-1 text-xs text-slate-500">{t("editor.historySubtitle")}</p>
          </div>

          <button
            type="button"
            aria-label={t("editor.closeHistory")}
            className="inline-flex items-center rounded-lg border border-transparent p-1.5 text-slate-500 transition hover:border-slate-200 hover:bg-slate-50 hover:text-slate-700"
            onClick={onClose}
          >
            <X size={16} strokeWidth={2} />
          </button>
        </div>

        <div className="overflow-y-auto px-4 py-4 lg:px-5">
          {queryHistory.length === 0 ? (
            <p className="text-sm text-slate-500">{t("editor.historyEmpty")}</p>
          ) : (
            <div className="grid gap-3">
              {queryHistory.map((entry) => (
                <button
                  key={entry.id}
                  type="button"
                  className="rounded-xl border border-slate-200 bg-slate-50/80 px-4 py-3 text-left transition hover:border-sky-300 hover:bg-sky-50"
                  onClick={() => onSelect(entry)}
                  title={t("editor.historyOpen")}
                  disabled={historyDisabled}
                >
                  <p className="text-sm font-medium text-slate-800">
                    {summarizeSql(entry.sql)}
                  </p>
                  <p className="mt-2 truncate text-xs text-slate-500">
                    {t("editor.historyPath")}: {entry.workspace_root_path}
                  </p>
                  <p className="mt-1 text-[11px] text-slate-400">
                    {formatHistoryTimestamp(entry.created_at)}
                  </p>
                </button>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>,
    document.body
  );
}
