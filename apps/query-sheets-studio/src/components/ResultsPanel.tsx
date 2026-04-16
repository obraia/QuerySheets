import { useMemo } from "react";
import type { QueryResult } from "../types/query";

const EXCEL_EPOCH_UTC_MS = Date.UTC(1899, 11, 30);
const DAY_MS = 24 * 60 * 60 * 1000;

const dateOnlyFormatter = new Intl.DateTimeFormat("pt-BR", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  timeZone: "UTC"
});

const dateTimeFormatter = new Intl.DateTimeFormat("pt-BR", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
  timeZone: "UTC"
});

function parseNumericString(raw: string): number | null {
  const value = raw.trim();
  if (!/^-?\d+(\.\d+)?$/.test(value)) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function looksLikeExcelSerial(value: number): boolean {
  return value >= 20000 && value <= 80000;
}

function hasFractionalTime(value: number): boolean {
  return Math.abs(value - Math.trunc(value)) > 1e-8;
}

function columnNameSuggestsDate(columnName: string): boolean {
  const normalized = columnName.toLowerCase();
  return [
    "date",
    "data",
    "time",
    "hora",
    "timestamp",
    "created",
    "updated",
    "inserido",
    "inclusao",
    "emissao",
    "nascimento"
  ].some((term) => normalized.includes(term));
}

function excelSerialToUtcDate(serial: number): Date {
  return new Date(EXCEL_EPOCH_UTC_MS + serial * DAY_MS);
}

type ResultsPanelProps = {
  result: QueryResult | null;
  resultMeta: string;
  error: string | null;
  isLoading: boolean;
  currentPage: number;
  pageSize: number;
  pageSizeOptions: number[];
  hasNextPage: boolean;
  totalRows: number;
  onPreviousPage: () => void;
  onNextPage: () => void;
  onPageSizeChange: (nextPageSize: number) => void;
};

export function ResultsPanel({
  result,
  resultMeta,
  error,
  isLoading,
  currentPage,
  pageSize,
  pageSizeOptions,
  hasNextPage,
  totalRows,
  onPreviousPage,
  onNextPage,
  onPageSizeChange
}: ResultsPanelProps): JSX.Element {
  const columns = result?.columns ?? [];
  const rows = result?.rows ?? [];
  const paginationDisabled = isLoading || !result;

  const dateLikeColumnByIndex = useMemo(() => {
    return columns.map((columnName, columnIndex) => {
      const headerHint = columnNameSuggestsDate(columnName);
      const samples = rows
        .map((row) => row[columnIndex] ?? "")
        .filter((value) => value.trim().length > 0)
        .slice(0, 120);

      if (samples.length === 0) {
        return false;
      }

      const serialSamples = samples
        .map(parseNumericString)
        .filter((value): value is number => value !== null && looksLikeExcelSerial(value));

      if (serialSamples.length === 0) {
        return false;
      }

      const serialRatio = serialSamples.length / samples.length;
      const timeRatio =
        serialSamples.filter((value) => hasFractionalTime(value)).length / serialSamples.length;

      if (headerHint) {
        return serialRatio >= 0.5;
      }

      return serialRatio >= 0.9 && timeRatio >= 0.25;
    });
  }, [columns, rows]);

  const displayColumnName = (column: string): string => {
    const normalized = column.trim();
    return normalized.length > 0 ? normalized : "(unnamed)";
  };

  const formatCellValue = (value: string, columnIndex: number): string => {
    if (!dateLikeColumnByIndex[columnIndex]) {
      return value;
    }

    const numeric = parseNumericString(value);
    if (numeric === null || !looksLikeExcelSerial(numeric)) {
      return value;
    }

    const date = excelSerialToUtcDate(numeric);
    if (Number.isNaN(date.getTime())) {
      return value;
    }

    return hasFractionalTime(numeric)
      ? dateTimeFormatter.format(date)
      : dateOnlyFormatter.format(date);
  };

  return (
    <section className="grid h-full min-h-0 grid-rows-[auto_auto_minmax(0,1fr)] rounded-2xl border border-slate-200/70 bg-white/90 shadow-[0_16px_40px_-26px_rgba(15,23,42,0.45)] backdrop-blur-md">
      <header className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-100 px-4 py-3 lg:px-5">
        <p className="text-sm font-semibold text-slate-800">Results</p>

        <div className="flex flex-wrap items-center justify-end gap-3">
          {isLoading && (
            <p className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-500" aria-live="polite">
              <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-teal-500" />
              Executing...
            </p>
          )}

          <p className="text-xs text-slate-500">{resultMeta}</p>

          <div className="inline-flex items-center gap-2 text-xs text-slate-600">
            <span>Rows per page</span>
            <select
              className="rounded-lg border border-slate-300 bg-white px-2 py-1 text-xs text-slate-700 outline-none transition focus:border-sky-400 disabled:cursor-not-allowed disabled:opacity-60"
              value={pageSize}
              onChange={(event) => onPageSizeChange(Number(event.target.value))}
              disabled={paginationDisabled}
            >
              {pageSizeOptions.map((size) => (
                <option key={size} value={size}>
                  {size}
                </option>
              ))}
            </select>
          </div>

          <div className="inline-flex items-center gap-2 text-xs">
            <span className="text-slate-600">Page {currentPage}</span>
            <span className="text-slate-500">· {totalRows} total</span>
            <button
              type="button"
              className="rounded-lg border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-700 transition hover:border-slate-400 disabled:cursor-not-allowed disabled:opacity-60"
              onClick={onPreviousPage}
              disabled={paginationDisabled || currentPage <= 1}
            >
              Previous
            </button>
            <button
              type="button"
              className="rounded-lg border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-700 transition hover:border-slate-400 disabled:cursor-not-allowed disabled:opacity-60"
              onClick={onNextPage}
              disabled={paginationDisabled || !hasNextPage}
            >
              Next
            </button>
          </div>
        </div>
      </header>

      {error && (
        <div className="mx-4 mt-3 rounded-xl border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700 lg:mx-5">
          {error}
        </div>
      )}

      <div className="min-h-0 overflow-y-auto overflow-x-auto px-4 pb-4 pt-3 lg:px-5 lg:pb-5">
        {!error && !result && !isLoading && (
          <div className="flex h-full min-h-[140px] items-center justify-center rounded-xl border border-dashed border-slate-300 bg-slate-50/70 px-4 text-sm text-slate-500">
            Run a query to see results.
          </div>
        )}

        {!error && !result && isLoading && (
          <div className="flex h-full min-h-[140px] items-center justify-center rounded-xl border border-dashed border-slate-300 bg-slate-50/70 px-4 text-sm text-slate-500">
            <span className="inline-flex items-center gap-2">
              <span className="h-3 w-3 animate-spin rounded-full border-2 border-slate-300 border-t-teal-500" />
              Executing query...
            </span>
          </div>
        )}

        {!error && result && columns.length === 0 && (
          <div className="rounded-xl border border-dashed border-slate-300 bg-slate-50/70 px-4 py-8 text-center text-sm text-slate-500">
            No columns in result.
          </div>
        )}

        {!error && result && columns.length > 0 && (
          <table className="min-w-full border-collapse overflow-hidden rounded-xl text-left text-xs">
            <thead>
              <tr>
                {columns.map((column, index) => {
                  const label = displayColumnName(column);

                  return (
                    <th
                      key={`column-${index}-${column}`}
                      title={label}
                      className="sticky top-0 z-10 border border-slate-200 bg-gradient-to-r from-teal-50 to-cyan-50 px-3 py-2 font-semibold text-slate-700"
                    >
                      <span className="block max-w-[280px] truncate whitespace-nowrap">{label}</span>
                    </th>
                  );
                })}
              </tr>
            </thead>

            <tbody>
              {rows.map((row, rowIndex) => (
                <tr key={`row-${rowIndex}`} className="odd:bg-white even:bg-slate-50/80">
                  {row.map((value, cellIndex) => {
                    const displayValue = formatCellValue(value, cellIndex);

                    return (
                    <td
                      key={`row-${rowIndex}-cell-${cellIndex}`}
                      className="whitespace-nowrap border border-slate-100 px-3 py-2 font-mono text-[12px] text-slate-700"
                      title={displayValue !== value ? `${displayValue} (raw: ${value})` : undefined}
                    >
                      {displayValue}
                    </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

    </section>
  );
}