import type { WorkspaceOverview } from "../types/query";
import { useI18n } from "../i18n";

type ExplorerPanelProps = {
  workspace: WorkspaceOverview | null;
  onPickSheet: (alias: string, sheet: string) => void;
};

export function ExplorerPanel({ workspace, onPickSheet }: ExplorerPanelProps): JSX.Element {
  const { t } = useI18n();

  return (
    <aside className="grid h-full min-h-0 grid-rows-[auto_auto_minmax(0,1fr)] rounded-2xl border border-slate-200/70 bg-white/85 p-4 shadow-[0_16px_40px_-26px_rgba(15,23,42,0.45)] backdrop-blur-md lg:p-5">
      <div className="mb-4 flex items-center justify-between">
        <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500">{t("explorer.title")}</p>
        <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-medium text-slate-600">
          {workspace?.files.length ?? 0} {t("explorer.files")}
        </span>
      </div>

      <div className="mb-4 rounded-xl border border-slate-200 bg-slate-50/80 p-3 text-sm text-slate-600">
        {workspace
          ? t("explorer.loaded", { count: workspace.files.length, cache: workspace.cached_tables })
          : t("explorer.noFolder")}
      </div>

      {!workspace && (
        <div className="rounded-xl border border-dashed border-slate-300 p-4 text-sm text-slate-500">
          {t("explorer.openHint")}
        </div>
      )}

      <div className="min-h-0 space-y-3 overflow-auto pr-1">
        {workspace?.files.map((workbook) => (
          <section key={`${workbook.alias}:${workbook.file_name}`} className="rounded-xl border border-slate-200 bg-white">
            <header className="border-b border-slate-100 px-3 py-2">
              <p className="truncate text-sm font-semibold text-slate-800" title={workbook.file_name}>
                {workbook.alias}
              </p>
              <p className="truncate text-xs text-slate-500" title={workbook.file_name}>
                {workbook.file_name}
              </p>
            </header>

            <div className="flex flex-wrap gap-2 p-3">
              {workbook.sheets.map((sheet) => (
                <button
                  type="button"
                  key={`${workbook.alias}:${sheet}`}
                  className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-xs font-medium text-slate-700 transition hover:border-teal-300 hover:text-teal-700"
                  title={t("explorer.useSheet", { table: `${workbook.alias}.${sheet}` })}
                  onClick={() => onPickSheet(workbook.alias, sheet)}
                >
                  {sheet}
                </button>
              ))}
            </div>
          </section>
        ))}
      </div>
    </aside>
  );
}