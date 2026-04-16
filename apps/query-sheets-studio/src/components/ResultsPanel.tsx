import type { QueryResult } from "../types/query";

type ResultsPanelProps = {
  result: QueryResult | null;
  resultMeta: string;
  error: string | null;
};

export function ResultsPanel({ result, resultMeta, error }: ResultsPanelProps): JSX.Element {
  const columns = result?.columns ?? [];
  const rows = result?.rows ?? [];

  return (
    <section className="grid min-h-[260px] grid-rows-[auto_auto_minmax(0,1fr)] rounded-2xl border border-slate-200/70 bg-white/90 shadow-[0_16px_40px_-26px_rgba(15,23,42,0.45)] backdrop-blur-md">
      <header className="flex items-center justify-between border-b border-slate-100 px-4 py-3 lg:px-5">
        <p className="text-sm font-semibold text-slate-800">Results</p>
        <p className="text-xs text-slate-500">{resultMeta}</p>
      </header>

      {error && (
        <div className="mx-4 mt-3 rounded-xl border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700 lg:mx-5">
          {error}
        </div>
      )}

      <div className="overflow-auto px-4 pb-4 pt-3 lg:px-5 lg:pb-5">
        {!error && !result && (
          <div className="flex h-full min-h-[140px] items-center justify-center rounded-xl border border-dashed border-slate-300 bg-slate-50/70 px-4 text-sm text-slate-500">
            Run a query to see results.
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
                {columns.map((column) => (
                  <th
                    key={column}
                    className="sticky top-0 z-10 border border-slate-200 bg-gradient-to-r from-teal-50 to-cyan-50 px-3 py-2 font-semibold text-slate-700"
                  >
                    {column}
                  </th>
                ))}
              </tr>
            </thead>

            <tbody>
              {rows.map((row, rowIndex) => (
                <tr key={`row-${rowIndex}`} className="odd:bg-white even:bg-slate-50/80">
                  {row.map((value, cellIndex) => (
                    <td
                      key={`row-${rowIndex}-cell-${cellIndex}`}
                      className="border border-slate-100 px-3 py-2 font-mono text-[12px] text-slate-700"
                    >
                      {value}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}