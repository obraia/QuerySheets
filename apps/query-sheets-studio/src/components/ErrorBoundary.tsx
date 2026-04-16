import { Component, type ErrorInfo, type ReactNode } from "react";

type ErrorBoundaryProps = {
  children: ReactNode;
};

type ErrorBoundaryState = {
  hasError: boolean;
  errorMessage: string;
};

export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  public constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = {
      hasError: false,
      errorMessage: ""
    };
  }

  public static getDerivedStateFromError(error: unknown): ErrorBoundaryState {
    const message = error instanceof Error ? error.message : String(error);
    return {
      hasError: true,
      errorMessage: message
    };
  }

  public componentDidCatch(error: unknown, errorInfo: ErrorInfo): void {
    console.error("Unhandled render error", error, errorInfo);
  }

  public render(): ReactNode {
    if (this.state.hasError) {
      return (
        <div className="flex min-h-screen items-center justify-center bg-[radial-gradient(circle_at_15%_20%,#eff6ff_0%,#f8fafc_35%,#fff7ed_100%)] px-4 py-8 text-slate-900">
          <div className="w-full max-w-xl rounded-2xl border border-red-200 bg-white/95 p-6 shadow-[0_16px_40px_-26px_rgba(15,23,42,0.45)] backdrop-blur-md">
            <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-red-500">Application Error</p>
            <h1 className="mt-1 text-2xl font-bold tracking-tight text-slate-900">A tela encontrou um erro inesperado</h1>
            <p className="mt-2 text-sm text-slate-600">
              O app foi protegido por um Error Boundary para evitar tela branca total. Recarregue e tente novamente.
            </p>

            <div className="mt-4 rounded-xl border border-red-100 bg-red-50 px-3 py-2 font-mono text-xs text-red-700">
              {this.state.errorMessage || "Unknown render error"}
            </div>

            <div className="mt-5">
              <button
                type="button"
                className="rounded-xl border border-transparent bg-gradient-to-r from-teal-500 to-cyan-500 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:from-teal-600 hover:to-cyan-600"
                onClick={() => window.location.reload()}
              >
                Recarregar Aplicação
              </button>
            </div>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}