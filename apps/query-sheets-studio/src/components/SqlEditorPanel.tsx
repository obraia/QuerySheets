import { useMemo } from "react";
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
import type { WorkspaceOverview } from "../types/query";

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
  "COUNT",
  "SUM",
  "AVG",
  "MIN",
  "MAX",
  "STDDEV",
  "CAST"
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
  isRunning: boolean;
  workspace: WorkspaceOverview | null;
};

export function SqlEditorPanel({
  sql,
  onSqlChange,
  onRunQuery,
  isRunning,
  workspace
}: SqlEditorPanelProps): JSX.Element {
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
    <section className="rounded-2xl border border-slate-200/70 bg-white/90 shadow-[0_16px_40px_-26px_rgba(15,23,42,0.45)] backdrop-blur-md">
      <header className="flex items-center justify-between border-b border-slate-100 px-4 py-3 lg:px-5">
        <p className="text-sm font-semibold text-slate-800">query.sql</p>
        <p className="text-xs text-slate-500">
          {isRunning
            ? "Executing"
            : "Cmd/Ctrl + Enter or Shift + Enter run · Cmd/Ctrl/Alt + Space autocomplete"}
        </p>
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
  );
}