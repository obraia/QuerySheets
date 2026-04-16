import CodeMirror from "@uiw/react-codemirror";
import { sql as sqlLanguage } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import { keymap } from "@codemirror/view";

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
  const editorExtensions = [
    sqlLanguage(),
    keymap.of([
      {
        key: "Mod-Enter",
        run: () => {
          void onRunQuery();
          return true;
        }
      }
    ])
  ];

  return (
    <section className="rounded-2xl border border-slate-200/70 bg-white/90 shadow-[0_16px_40px_-26px_rgba(15,23,42,0.45)] backdrop-blur-md">
      <header className="flex items-center justify-between border-b border-slate-100 px-4 py-3 lg:px-5">
        <p className="text-sm font-semibold text-slate-800">query.sql</p>
        <p className="text-xs text-slate-500">{isRunning ? "Executing" : "Ctrl/Cmd + Enter to run"}</p>
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
          autocompletion: true
        }}
        editable={!isRunning}
        extensions={editorExtensions}
        onChange={(value) => onSqlChange(value)}
        className="sql-editor"
      />
    </section>
  );
}