import { basename } from "node:path";

/** Bounds the retry of the first connection. */
export interface RetryPolicy {
  /** Total tries, including the first. */
  attempts: number;
  /** Ceiling of the first sleep, ms. */
  initialDelayMs: number;
  /** Ceiling of any sleep, ms. */
  maxDelayMs: number;
  /** Total time that may be spent waiting, ms. */
  budgetMs: number;
}

/**
 * Everything a connection needs. Start from {@link defaultConfig} and override.
 *
 * There is deliberately no switch that turns verification off and no free-form
 * option string: a parameter handed through a string can be dropped on the way
 * to the driver, and a dropped CA turns a verified connection into one that
 * does not verify.
 */
export interface NatsConfig {
  /** The broker: one nats:// or tls:// URL. The server certificate must carry its host name. */
  url: string;
  /** The server CA. When set the connection is TLS and trusts this file and nothing else. A tls:// URL requires it. */
  caFile?: string;
  /** Workload certificate and key (a SPIFFE identity the broker maps to a user); both or neither. Read again for every connection. */
  certFile?: string;
  keyFile?: string;
  /** Projected ServiceAccount token the callout accepts as the NATS auth token. Read again for every connection. */
  tokenFile?: string;
  /** Connection name shown by the broker. Default: the program name. */
  name: string;
  connectTimeoutMs: number;
  reconnectWaitMs: number;
  pingIntervalMs: number;
  drainTimeoutMs: number;
  /** Bounds the retry of the first connection; after it the driver reconnects without limit. */
  connect: RetryPolicy;
}

export class ConfigError extends Error {
  constructor(readonly problems: string[]) {
    super(`natsclient: invalid configuration:\n  - ${problems.join("\n  - ")}`);
    this.name = "ConfigError";
  }
}

export const defaultRetryPolicy = (): RetryPolicy => ({
  attempts: 5,
  initialDelayMs: 200,
  maxDelayMs: 5_000,
  budgetMs: 30_000,
});

/** The contract's defaults; fill url and a credential. */
export function defaultConfig(): NatsConfig {
  return {
    url: "",
    name: programName(),
    connectTimeoutMs: 5_000,
    reconnectWaitMs: 1_000,
    pingIntervalMs: 30_000,
    drainTimeoutMs: 10_000,
    connect: defaultRetryPolicy(),
  };
}

function programName(): string {
  return basename(process.argv[1] ?? "node") || "node";
}

/** Removes any user info from a URL, for messages and logs. */
export function redactUrl(raw: string): string {
  try {
    const u = new URL(raw);
    if (!u.username && !u.password) return raw;
    u.username = "";
    u.password = "";
    return u.toString();
  } catch {
    return raw.replace(/\/\/[^@/]*@/, "//");
  }
}

/** Reports every problem, not just the first. */
export function validateConfig(c: NatsConfig): void {
  const p: string[] = [];
  let scheme = "";
  if (!c.url) {
    p.push("URL is required (NATS_URL)");
  } else {
    if (/[, ]/.test(c.url)) {
      p.push(`URL "${redactUrl(c.url)}": exactly one URL is accepted (the broker's Service)`);
    }
    let u: URL | undefined;
    try {
      u = new URL(c.url);
    } catch {
      u = undefined;
    }
    if (!u || u.hostname === "") {
      p.push("URL is not a nats:// or tls:// URL with a host");
    } else {
      const s = u.protocol.replace(/:$/, "");
      if (s !== "nats" && s !== "tls")
        p.push(`URL scheme "${s}" is refused; only nats:// and tls:// are accepted`);
      else scheme = s;
      if (u.username || u.password) {
        p.push("credentials in the URL are refused; use the token file or the certificate");
      }
    }
  }
  if (scheme === "tls" && !c.caFile) {
    p.push(
      "a tls:// URL needs the server CA file (NATS_CA_FILE): verification has nothing to verify against without it",
    );
  }
  if (Boolean(c.certFile) !== Boolean(c.keyFile)) p.push("client certificate and key go together");
  const cert = Boolean(c.certFile) || Boolean(c.keyFile);
  if (cert && c.tokenFile) {
    p.push("a certificate and a token file are two identities; give one");
  } else if (!cert && !c.tokenFile) {
    p.push(
      "a credential is required: a certificate and key (NATS_CERT_FILE, NATS_KEY_FILE) or a token file (NATS_TOKEN_FILE)",
    );
  }
  if (c.certFile && c.keyFile && !c.caFile) {
    p.push("a client certificate needs the server CA file: it is only ever sent over a verified connection");
  }
  if (!(c.connectTimeoutMs > 0 && c.reconnectWaitMs > 0 && c.pingIntervalMs > 0 && c.drainTimeoutMs > 0)) {
    p.push("connectTimeoutMs, reconnectWaitMs, pingIntervalMs and drainTimeoutMs must be positive");
  }
  const r = c.connect;
  if (!(r.attempts >= 1 && r.initialDelayMs > 0 && r.maxDelayMs >= r.initialDelayMs && r.budgetMs > 0)) {
    p.push("retry policy: attempts>=1, 0<initialDelayMs<=maxDelayMs, budgetMs>0");
  }
  if (p.length > 0) throw new ConfigError(p);
}

const UNIT_MS: Record<string, number> = {
  ns: 1e-6,
  us: 1e-3,
  µs: 1e-3,
  μs: 1e-3,
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
};

/** A Go duration: "500ms", "30s", "1m30s", "1.5h" (a "0" is accepted too). Returns milliseconds. */
export function parseDuration(text: string): number {
  if (text === "0") return 0;
  const full = /^(?:\d+(?:\.\d+)?(?:ns|us|µs|μs|ms|s|m|h))+$/;
  if (!full.test(text)) throw new Error(`"${text}" is not a duration (use 500ms, 30s, 1m30s, 1h)`);
  let ms = 0;
  for (const m of text.matchAll(/(\d+(?:\.\d+)?)(ns|us|µs|μs|ms|s|m|h)/g)) {
    ms += Number(m[1]) * (UNIT_MS[m[2] as string] as number);
  }
  return Math.round(ms);
}

/**
 * Builds a configuration from the contract's environment, starting from
 * {@link defaultConfig}. The result is validated.
 */
export function configFromEnv(env: Record<string, string | undefined> = process.env): NatsConfig {
  const problems: string[] = [];
  const get = (k: string) => (env[k] === "" ? undefined : env[k]);
  const num = (k: string, into: (n: number) => void, parse: (s: string) => number) => {
    const v = get(k);
    if (v === undefined) return;
    try {
      const n = parse(v);
      if (!Number.isFinite(n)) throw new Error("not a number");
      into(n);
    } catch (e) {
      problems.push(`${k}="${v}": ${(e as Error).message}`);
    }
  };
  const int = (s: string) => {
    if (!/^-?\d+$/.test(s)) throw new Error("not a whole number");
    return Number(s);
  };

  const c = defaultConfig();
  c.url = get("NATS_URL") ?? "";
  c.caFile = get("NATS_CA_FILE");
  c.certFile = get("NATS_CERT_FILE");
  c.keyFile = get("NATS_KEY_FILE");
  c.tokenFile = get("NATS_TOKEN_FILE");
  const name = get("NATS_CLIENT_NAME");
  if (name) c.name = name;

  num("NATS_CLIENT_CONNECT_TIMEOUT", (n) => (c.connectTimeoutMs = n), parseDuration);
  num("NATS_CLIENT_RECONNECT_WAIT", (n) => (c.reconnectWaitMs = n), parseDuration);
  num("NATS_CLIENT_PING_INTERVAL", (n) => (c.pingIntervalMs = n), parseDuration);
  num("NATS_CLIENT_DRAIN_TIMEOUT", (n) => (c.drainTimeoutMs = n), parseDuration);
  num("NATS_CLIENT_RETRY_ATTEMPTS", (n) => (c.connect.attempts = n), int);
  num("NATS_CLIENT_RETRY_MAX_DELAY", (n) => (c.connect.maxDelayMs = n), parseDuration);
  num("NATS_CLIENT_RETRY_BUDGET", (n) => (c.connect.budgetMs = n), parseDuration);

  if (problems.length > 0) throw new ConfigError(problems);
  validateConfig(c);
  return c;
}

/** A copy that is safe to log or serialise: no credential, only which files are in use. */
export function redactConfig(c: NatsConfig): Record<string, unknown> {
  return {
    url: redactUrl(c.url),
    tls: Boolean(c.caFile),
    caFile: c.caFile,
    certFile: c.certFile,
    keyFile: c.keyFile,
    tokenFile: c.tokenFile,
    name: c.name,
  };
}
