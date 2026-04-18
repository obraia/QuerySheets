import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

export type AppLanguage = "en" | "pt-BR";

type Dictionary = Record<string, string>;

const LANGUAGE_PREF_KEY = "querysheets.language";

const EN: Dictionary = {
  "title.workspace": "Workspace",
  "title.appName": "QuerySheets Studio",
  "title.openFolder": "Open Folder",
  "title.refresh": "Refresh",
  "title.runQuery": "Run Query",
  "title.running": "Running...",
  "title.parallel": "Parallel",
  "title.language": "Language",

  "explorer.title": "Explorer",
  "explorer.files": "files",
  "explorer.loaded": "{count} workbook(s) loaded - cache {cache}",
  "explorer.noFolder": "No folder open",
  "explorer.openHint": "Open a folder to inspect aliases and worksheet names.",
  "explorer.useSheet": "Use {table} in FROM/JOIN",

  "editor.loading": "Loading editor...",
  "editor.executing": "Executing",
  "editor.shortcuts": "Cmd/Ctrl + Enter or Shift + Enter run · Cmd/Ctrl/Alt + Space autocomplete",
  "editor.export": "Export query result",
  "editor.exportOpenFolder": "Open a folder before exporting",
  "editor.exportFormat": "Export Format",
  "editor.history": "Recent queries",
  "editor.historySubtitle": "Pick a previous execution to reopen its workspace and SQL.",
  "editor.historyEmpty": "Successful executions will appear here.",
  "editor.historyOpen": "Open saved workspace and query",
  "editor.historyPath": "Workspace",
  "editor.closeHistory": "Close recent queries",

  "results.title": "Results",
  "results.executing": "Executing...",
  "results.rowsPerPage": "Rows per page",
  "results.page": "Page",
  "results.total": "{count} total",
  "results.previous": "Previous",
  "results.next": "Next",
  "results.runToSee": "Run a query to see results.",
  "results.executingQuery": "Executing query...",
  "results.noColumns": "No columns in result.",

  "error.badge": "Application Error",
  "error.title": "The screen encountered an unexpected error",
  "error.description": "The app was protected by an Error Boundary to avoid a full white screen. Reload and try again.",
  "error.unknown": "Unknown render error",
  "error.reload": "Reload Application",

  "status.ready": "Ready",
  "status.noQuery": "No query executed",
  "status.folderOpened": "Folder opened",
  "status.workspaceRefreshed": "Workspace refreshed",
  "status.openFolderBeforeRefresh": "Open a folder before refreshing",
  "status.loadedSheet": "Loaded {table} into editor",
  "status.typeQueryFirst": "Type a query first",
  "status.openFolderBeforeQuery": "Open a folder before querying",
  "status.runningQuery": "Running query...",
  "status.queryExecuted": "Query executed",
  "status.queryFailed": "Query failed",
  "status.historyLoaded": "Query history restored",
  "status.openFolderBeforeExport": "Open a folder before exporting",
  "status.exporting": "Exporting {format}...",
  "status.exportCanceled": "Export canceled",
  "status.exported": "Exported {rows} row(s) to {path} in {elapsed} ms",
  "status.exportFailed": "Export failed: {error}",
  "status.parallelEnabled": "Parallel execution enabled",
  "status.parallelDisabled": "Parallel execution disabled",
  "status.parallelUnavailable": "Parallel toggle unavailable: {error}",
  "status.dragNoFolder": "No folder found in drop",
  "status.dragResolveFailed": "Could not resolve folder from dropped item",
  "status.dragUnavailable": "Drag-and-drop unavailable: {error}",

  "meta.rows": "{rows} total row(s) in {elapsed} ms{suffix}",
  "meta.truncated": " (truncated)",
  "dropzone.hint": "Drop folder to open workspace"
};

const PT_BR: Dictionary = {
  "title.workspace": "Workspace",
  "title.appName": "QuerySheets Studio",
  "title.openFolder": "Abrir Pasta",
  "title.refresh": "Atualizar",
  "title.runQuery": "Executar Query",
  "title.running": "Executando...",
  "title.parallel": "Paralelo",
  "title.language": "Idioma",

  "explorer.title": "Explorador",
  "explorer.files": "arquivos",
  "explorer.loaded": "{count} planilha(s) carregada(s) - cache {cache}",
  "explorer.noFolder": "Nenhuma pasta aberta",
  "explorer.openHint": "Abra uma pasta para inspecionar aliases e nomes de planilhas.",
  "explorer.useSheet": "Use {table} no FROM/JOIN",

  "editor.loading": "Carregando editor...",
  "editor.executing": "Executando",
  "editor.shortcuts": "Cmd/Ctrl + Enter ou Shift + Enter executa · Cmd/Ctrl/Alt + Espaço autocompleta",
  "editor.export": "Exportar resultado da query",
  "editor.exportOpenFolder": "Abra uma pasta antes de exportar",
  "editor.exportFormat": "Formato de Exportação",
  "editor.history": "Queries recentes",
  "editor.historySubtitle": "Escolha uma execução anterior para reabrir seu workspace e SQL.",
  "editor.historyEmpty": "Execuções bem-sucedidas aparecerão aqui.",
  "editor.historyOpen": "Abrir workspace e query salvos",
  "editor.historyPath": "Workspace",
  "editor.closeHistory": "Fechar queries recentes",

  "results.title": "Resultados",
  "results.executing": "Executando...",
  "results.rowsPerPage": "Linhas por página",
  "results.page": "Página",
  "results.total": "{count} total",
  "results.previous": "Anterior",
  "results.next": "Próxima",
  "results.runToSee": "Execute uma query para ver resultados.",
  "results.executingQuery": "Executando query...",
  "results.noColumns": "Nenhuma coluna no resultado.",

  "error.badge": "Erro da Aplicação",
  "error.title": "A tela encontrou um erro inesperado",
  "error.description": "O app foi protegido por um Error Boundary para evitar tela branca total. Recarregue e tente novamente.",
  "error.unknown": "Erro de renderização desconhecido",
  "error.reload": "Recarregar Aplicação",

  "status.ready": "Pronto",
  "status.noQuery": "Nenhuma query executada",
  "status.folderOpened": "Pasta aberta",
  "status.workspaceRefreshed": "Workspace atualizado",
  "status.openFolderBeforeRefresh": "Abra uma pasta antes de atualizar",
  "status.loadedSheet": "{table} carregada no editor",
  "status.typeQueryFirst": "Digite uma query primeiro",
  "status.openFolderBeforeQuery": "Abra uma pasta antes de consultar",
  "status.runningQuery": "Executando query...",
  "status.queryExecuted": "Query executada",
  "status.queryFailed": "Falha na query",
  "status.historyLoaded": "Histórico de query restaurado",
  "status.openFolderBeforeExport": "Abra uma pasta antes de exportar",
  "status.exporting": "Exportando {format}...",
  "status.exportCanceled": "Exportação cancelada",
  "status.exported": "{rows} linha(s) exportada(s) para {path} em {elapsed} ms",
  "status.exportFailed": "Falha na exportação: {error}",
  "status.parallelEnabled": "Execução paralela ativada",
  "status.parallelDisabled": "Execução paralela desativada",
  "status.parallelUnavailable": "Toggle paralelo indisponível: {error}",
  "status.dragNoFolder": "Nenhuma pasta encontrada no drop",
  "status.dragResolveFailed": "Não foi possível resolver a pasta do item solto",
  "status.dragUnavailable": "Drag-and-drop indisponível: {error}",

  "meta.rows": "{rows} linha(s) total em {elapsed} ms{suffix}",
  "meta.truncated": " (truncado)",
  "dropzone.hint": "Solte a pasta para abrir o workspace"
};

const DICTS: Record<AppLanguage, Dictionary> = {
  en: EN,
  "pt-BR": PT_BR
};

type I18nContextValue = {
  language: AppLanguage;
  setLanguage: (language: AppLanguage) => void;
  t: (key: string, params?: Record<string, string | number>) => string;
};

const I18nContext = createContext<I18nContextValue | null>(null);

function resolveInitialLanguage(): AppLanguage {
  if (typeof window === "undefined") {
    return "en";
  }

  const saved = window.localStorage.getItem(LANGUAGE_PREF_KEY);
  if (saved === "pt-BR" || saved === "en") {
    return saved;
  }

  return "en";
}

function interpolate(template: string, params?: Record<string, string | number>): string {
  if (!params) {
    return template;
  }

  return template.replace(/\{([a-zA-Z0-9_]+)\}/g, (_match, key) => {
    const value = params[key];
    return value === undefined ? "" : String(value);
  });
}

function translate(language: AppLanguage, key: string, params?: Record<string, string | number>): string {
  const dict = DICTS[language] ?? EN;
  const fallback = EN[key] ?? key;
  const template = dict[key] ?? fallback;
  return interpolate(template, params);
}

export function I18nProvider({ children }: { children: ReactNode }): JSX.Element {
  const [language, setLanguage] = useState<AppLanguage>(resolveInitialLanguage);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem(LANGUAGE_PREF_KEY, language);
  }, [language]);

  const value = useMemo<I18nContextValue>(() => ({
    language,
    setLanguage,
    t: (key, params) => translate(language, key, params)
  }), [language]);

  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n(): I18nContextValue {
  const context = useContext(I18nContext);
  if (!context) {
    throw new Error("useI18n must be used within I18nProvider");
  }

  return context;
}

export function translateUnsafe(key: string, params?: Record<string, string | number>): string {
  const language = resolveInitialLanguage();
  return translate(language, key, params);
}
