// Loopback discovery: find moss nodes that opened a debug port on this machine.
//
// A node with `debug.enabled` answers /debug/hello with just enough to be
// recognised — that it is moss, which session, whether a token is needed — and
// nothing else. Everything past that point requires the token the node printed
// at startup.
//
// Scanning only works from a loopback origin. A page on https://scope.moss.surf
// cannot reach http://127.0.0.1 (mixed content, and Private Network Access on
// top of it), which is why the node serves this bundle itself: open the URL the
// node printed and discovery works because you are already on loopback.
const DEFAULT_PORTS = [7788, 7789, 7790, 8788, 8789];
const PROBE_TIMEOUT_MS = 250;

export function canDiscover() {
  return /^(localhost|127\.0\.0\.1|\[::1\]|::1)$/.test(location.hostname);
}

/**
 * @param {object} [opts]
 * @param {number[]} [opts.ports]  ports to probe
 * @param {number} [opts.timeout]
 * @returns {Promise<Array<{endpoint: string, info: object}>>}
 */
export async function discover({ ports = DEFAULT_PORTS, timeout = PROBE_TIMEOUT_MS } = {}) {
  if (!canDiscover()) return [];

  const probes = ports.map(async (port) => {
    const endpoint = `http://127.0.0.1:${port}`;
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), timeout);
    try {
      const res = await fetch(`${endpoint}/debug/hello`, {
        signal: ctl.signal,
        headers: { accept: "application/json" },
        // Never send cookies to a port that might not be moss at all.
        credentials: "omit",
        cache: "no-store",
      });
      if (!res.ok) return null;
      const info = await res.json();
      return info?.moss ? { endpoint, info } : null;
    } catch {
      return null; // closed port, wrong protocol, timeout — all the same answer
    } finally {
      clearTimeout(timer);
    }
  });

  return (await Promise.all(probes)).filter(Boolean);
}

/**
 * The token is not discoverable — that is the point. It arrives one of three
 * ways: in the URL the node printed, saved from a previous session, or typed.
 */
export function tokenFor(session) {
  const fromUrl = new URLSearchParams(location.search).get("token");
  if (fromUrl) {
    rememberToken(session, fromUrl);
    return fromUrl;
  }
  try {
    return localStorage.getItem(`moss-scope-token:${session}`) ?? localStorage.getItem("moss-scope-token") ?? "";
  } catch {
    return "";
  }
}

export function rememberToken(session, token) {
  try {
    localStorage.setItem(`moss-scope-token:${session}`, token);
    localStorage.setItem("moss-scope-token", token);
  } catch {
    /* private mode: the session simply will not be remembered */
  }
}
