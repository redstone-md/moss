// Query layer. Wraps TanStack Query's framework-agnostic core so panels get
// caching, deduplication, retries, background refetch and cancellation without
// any of them owning a fetch call.
//
// Two things this buys that hand-rolled polling does not:
//   * ten panels asking for the same metric issue one request;
//   * switching the lens cancels every in-flight request instead of letting a
//     slow response from the old source paint over the new one.
import { QueryClient, QueryObserver } from "@tanstack/query-core";

export class ScopeClient {
  constructor() {
    this.client = new QueryClient({
      defaultOptions: {
        queries: {
          staleTime: 2_000,
          gcTime: 5 * 60_000,
          retry: 1,
          retryDelay: (attempt) => Math.min(1_000 * 2 ** attempt, 8_000),
          refetchOnWindowFocus: false,
          networkMode: "always", // a loopback socket is up even when the WAN is not
        },
      },
    });
    /** @type {import("./DataSource.js").DataSource|null} */
    this.source = null;
    /** @type {Set<(src: any) => void>} */
    this._listeners = new Set();
    /** @type {Set<(metric: string, data: any) => void>} */
    this._pushListeners = new Set();
  }

  /** @param {(source: any) => void} fn */
  onSourceChange(fn) {
    this._listeners.add(fn);
    return () => this._listeners.delete(fn);
  }

  /**
   * Swap the active source. Everything in flight is cancelled and the cache is
   * cleared: results from two different sources must never mix in one view.
   * @param {import("./DataSource.js").DataSource} source
   */
  async setSource(source) {
    await this.source?.close();
    await this.client.cancelQueries();
    this.client.clear();

    this.source = source;
    await source.connect();

    // A live source pushes; feed those pushes straight into the cache so the
    // observing panels re-render through exactly the same path as a poll.
    if ("onPush" in source) {
      source.onPush = (metric, data) => {
        this.client.setQueryData([source.id, metric], data);
        for (const fn of this._pushListeners) fn(metric, data);
      };
    }

    for (const fn of this._listeners) fn(source);
  }

  /**
   * Listen to everything a live source pushes, including its own connection
   * state — which no panel observes but the chrome must react to.
   * @param {(metric: string, data: any) => void} fn
   */
  onPush(fn) {
    this._pushListeners.add(fn);
    return () => this._pushListeners.delete(fn);
  }

  /**
   * Observe one metric. Returns the observer so the caller can unsubscribe;
   * panels do that on destroy.
   * @param {import("./DataSource.js").QuerySpec} spec
   * @param {(result: {data: any, error: unknown, isPending: boolean, isFetching: boolean}) => void} onResult
   */
  observe(spec, onResult) {
    const source = this.source;
    if (!source) throw new Error("ScopeClient has no source");

    const interval = spec.refetchInterval ?? source.refetchInterval;
    const observer = new QueryObserver(this.client, {
      queryKey: [source.id, spec.metric, spec.params ?? null],
      queryFn: ({ signal }) => source.fetch(spec, signal),
      refetchInterval: interval > 0 ? interval : false,
    });

    const unsubscribe = observer.subscribe((result) => {
      onResult({
        data: result.data,
        error: result.error,
        isPending: result.isPending,
        isFetching: result.isFetching,
      });
    });
    // subscribe() does not deliver the current cached value synchronously.
    onResult(observer.getCurrentResult());

    return { observer, unsubscribe };
  }

  /** Force every visible panel to refetch now. */
  refetchAll() {
    return this.client.refetchQueries();
  }
}
