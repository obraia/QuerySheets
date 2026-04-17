import { useI18n } from "../i18n";

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
  const { language, setLanguage, t } = useI18n();

  return (
    <header className="rounded-2xl border border-slate-200/70 bg-white/85 p-4 shadow-[0_16px_40px_-26px_rgba(15,23,42,0.45)] backdrop-blur-md lg:p-5">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500">{t("title.workspace")}</p>
          <h1 className="mt-1 text-2xl font-bold tracking-tight text-slate-900">{t("title.appName")}</h1>
          <p className="mt-1 truncate text-sm text-slate-600" title={folderPath}>
            {folderPath}
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <label className="inline-flex items-center gap-2 rounded-xl border border-slate-300 bg-white px-3 py-2 text-xs font-medium text-slate-700">
            <span>{t("title.language")}</span>
            <select
              className="rounded-lg border border-slate-200 bg-white px-1 py-0.5 text-xs text-slate-700"
              value={language}
              onChange={(event) => setLanguage(event.target.value as "en" | "pt-BR")}
              disabled={isBusy}
            >
              <option value="en">EN</option>
              <option value="pt-BR">PT-BR</option>
            </select>
          </label>
          <label className="inline-flex items-center gap-2 rounded-xl border border-slate-300 bg-white px-3 py-2 text-xs font-medium text-slate-700">
            <input
              type="checkbox"
              className="h-4 w-4 accent-teal-600"
              checked={parallelEnabled}
              onChange={(event) => onParallelToggle(event.target.checked)}
              disabled={isBusy}
            />
            {t("title.parallel")}
          </label>
          <button
            type="button"
            className="rounded-xl border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-teal-300 hover:text-teal-700"
            onClick={onOpenFolder}
            disabled={isBusy}
          >
            {t("title.openFolder")}
          </button>
          <button
            type="button"
            className="rounded-xl border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-orange-300 hover:text-orange-700"
            onClick={onRefresh}
            disabled={isBusy}
          >
            {t("title.refresh")}
          </button>
          <button
            type="button"
            className="rounded-xl border border-transparent bg-gradient-to-r from-teal-500 to-cyan-500 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:from-teal-600 hover:to-cyan-600 disabled:cursor-not-allowed disabled:opacity-70"
            onClick={onRunQuery}
            disabled={isBusy}
          >
            {isRunning ? t("title.running") : t("title.runQuery")}
          </button>
        </div>
      </div>
    </header>
  );
}