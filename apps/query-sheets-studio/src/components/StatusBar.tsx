import type { StatusMessage } from "../types/query";

type StatusBarProps = {
  status: StatusMessage;
};

export function StatusBar({ status }: StatusBarProps): JSX.Element {
  return (
    <footer
      className={`rounded-xl border px-3 py-2 text-xs font-medium ${
        status.isError
          ? "border-red-200 bg-red-50 text-red-700"
          : "border-emerald-200 bg-emerald-50 text-emerald-700"
      }`}
    >
      {status.message}
    </footer>
  );
}