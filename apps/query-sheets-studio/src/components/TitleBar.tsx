type TitleBarProps = {
  folderPath: string;
  onOpenFolder: () => Promise<void>;
  onRefresh: () => Promise<void>;
  onRunQuery: () => Promise<void>;
  onParallelToggle: (enabled: boolean) => void;
  parallelEnabled: boolean;
  isBusy: boolean;
  isRunning: boolean;
};

export function TitleBar({
  folderPath,
  onOpenFolder,
  onRefresh,
  onRunQuery,
  onParallelToggle,
  parallelEnabled,
  isBusy,
  isRunning
}: TitleBarProps): JSX.Element {
  return (
    <header className="rounded-2xl border border-slate-200/70 bg-white/85 p-4 shadow-[0_16px_40px_-26px_rgba(15,23,42,0.45)] backdrop-blur-md lg:p-5">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500">Workspace</p>
          <h1 className="mt-1 text-2xl font-bold tracking-tight text-slate-900">QuerySheets Studio</h1>
          <p className="mt-1 truncate text-sm text-slate-600" title={folderPath}>
            {folderPath}
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <label className="inline-flex items-center gap-2 rounded-xl border border-slate-300 bg-white px-3 py-2 text-xs font-medium text-slate-700">
            <input
              type="checkbox"
              className="h-4 w-4 accent-teal-600"
              checked={parallelEnabled}
              onChange={(event) => onParallelToggle(event.target.checked)}
              disabled={isBusy}
            />
            Parallel
          </label>
          <button
            type="button"
            className="rounded-xl border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-teal-300 hover:text-teal-700"
            onClick={onOpenFolder}
            disabled={isBusy}
          >
            Open Folder
          </button>
          <button
            type="button"
            className="rounded-xl border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-orange-300 hover:text-orange-700"
            onClick={onRefresh}
            disabled={isBusy}
          >
            Refresh
          </button>
          <button
            type="button"
            className="rounded-xl border border-transparent bg-gradient-to-r from-teal-500 to-cyan-500 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:from-teal-600 hover:to-cyan-600 disabled:cursor-not-allowed disabled:opacity-70"
            onClick={onRunQuery}
            disabled={isBusy}
          >
            {isRunning ? "Running..." : "Run Query"}
          </button>
        </div>
      </div>
    </header>
  );
}