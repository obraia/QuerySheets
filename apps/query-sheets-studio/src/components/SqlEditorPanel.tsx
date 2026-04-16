import type { KeyboardEvent } from "react";

type SqlEditorPanelProps = {
  sql: string;
  onSqlChange: (value: string) => void;
  onRunQuery: () => Promise<void>;
  isRunning: boolean;
};

export function SqlEditorPanel({
  sql,
  onSqlChange,
  onRunQuery,
  isRunning
}: SqlEditorPanelProps): JSX.Element {
  async function handleKeyDown(event: KeyboardEvent<HTMLTextAreaElement>): Promise<void> {
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
      event.preventDefault();
      await onRunQuery();
    }
  }

  return (
    <section className="rounded-2xl border border-slate-200/70 bg-white/90 shadow-[0_16px_40px_-26px_rgba(15,23,42,0.45)] backdrop-blur-md">
      <header className="flex items-center justify-between border-b border-slate-100 px-4 py-3 lg:px-5">
        <p className="text-sm font-semibold text-slate-800">query.sql</p>
        <p className="text-xs text-slate-500">{isRunning ? "Executing" : "Ctrl/Cmd + Enter to run"}</p>
      </header>

      <textarea
        spellCheck={false}
        value={sql}
        onChange={(event) => onSqlChange(event.target.value)}
        onKeyDown={handleKeyDown}
        className="min-h-[210px] w-full resize-y border-0 bg-slate-950 px-4 py-3 font-mono text-sm leading-6 text-slate-100 outline-none ring-0 lg:px-5"
      />
    </section>
  );
}