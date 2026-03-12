import { Component, type ErrorInfo, type ReactNode } from 'react'

type AppErrorBoundaryProps = {
  children: ReactNode
}

type AppErrorBoundaryState = {
  errorMessage: string | null
}

export class AppErrorBoundary extends Component<
  AppErrorBoundaryProps,
  AppErrorBoundaryState
> {
  state: AppErrorBoundaryState = {
    errorMessage: null,
  }

  static getDerivedStateFromError(error: Error): AppErrorBoundaryState {
    return {
      errorMessage: error.message || 'Unknown desktop shell error',
    }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('Moss Chat Dev render failure', error, info)
  }

  render() {
    if (this.state.errorMessage) {
      return (
        <main className="shell loading">
          <section className="error-panel">
            <p className="eyebrow">Render error</p>
            <h1>Desktop shell crashed during render</h1>
            <p>{this.state.errorMessage}</p>
          </section>
        </main>
      )
    }

    return this.props.children
  }
}
