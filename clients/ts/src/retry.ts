import type { RetryPolicy } from "./config.js";

const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve) => {
    const t = setTimeout(done, ms);
    signal?.addEventListener("abort", done, { once: true });
    function done() {
      clearTimeout(t);
      signal?.removeEventListener("abort", done);
      resolve();
    }
  });

/** Exponential backoff with full jitter. `attempt` is 1 for the first retry. */
export function backoffMs(p: RetryPolicy, attempt: number): number {
  const ceil = Math.min(p.maxDelayMs, p.initialDelayMs * 2 ** (attempt - 1));
  return Math.floor(Math.random() * (ceil + 1));
}

/** Throws when the policy is out of range. */
export function validateRetryPolicy(p: RetryPolicy): void {
  if (!(p.attempts >= 1 && p.initialDelayMs > 0 && p.maxDelayMs >= p.initialDelayMs && p.budgetMs > 0)) {
    throw new Error(
      `natsclient: retry policy ${JSON.stringify(p)}: attempts>=1, 0<initialDelayMs<=maxDelayMs, budgetMs>0`,
    );
  }
}

/**
 * Runs `fn`, repeating it while it fails with a retryable error (see
 * {@link isRetryable}), stopping at `attempts` or `budgetMs`, whichever is
 * first. `fn` must be safe to run again.
 */
export async function retry<T>(policy: RetryPolicy, fn: () => Promise<T>, signal?: AbortSignal): Promise<T> {
  validateRetryPolicy(policy);
  const start = Date.now();
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (signal?.aborted || !isRetryable(err) || attempt >= policy.attempts) throw err;
      const delay = backoffMs(policy, attempt);
      if (Date.now() - start + delay > policy.budgetMs) throw err;
      await sleep(delay, signal);
      if (signal?.aborted) throw err;
    }
  }
}

const NEVER_CODES = new Set([
  // certificate verification and TLS handshake rejection: a second try cannot fix them
  "DEPTH_ZERO_SELF_SIGNED_CERT",
  "SELF_SIGNED_CERT_IN_CHAIN",
  "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
  "UNABLE_TO_GET_ISSUER_CERT_LOCALLY",
  "CERT_HAS_EXPIRED",
  "CERT_NOT_YET_VALID",
  "ERR_TLS_CERT_ALTNAME_INVALID",
  "ERR_TLS_INVALID_PROTOCOL_VERSION",
]);
const NETWORK_CODES = new Set([
  "ECONNRESET",
  "ECONNREFUSED",
  "EPIPE",
  "ETIMEDOUT",
  "EAI_AGAIN",
  "ECONNABORTED",
  "EHOSTUNREACH",
]);
const NEVER_NAMES = new Set([
  "AuthorizationError",
  "UserAuthenticationExpiredError",
  "PermissionViolationError",
]);
const RETRY_NAMES = new Set(["ConnectionError", "TimeoutError", "ClosedConnectionError"]);

function chain(err: unknown): Error[] {
  const out: Error[] = [];
  for (let e: unknown = err; e instanceof Error && out.length < 8; e = (e as { cause?: unknown }).cause) {
    out.push(e);
  }
  return out;
}

/**
 * Reports whether `err` means the broker was not reachable (down, restarting,
 * not accepting yet), which a later try can fix.
 *
 * Never retryable: an authorization or authentication failure, a certificate
 * verification failure (a second try cannot fix either), a permissions
 * violation, and the caller's own abort.
 */
export function isRetryable(err: unknown): boolean {
  const errs = chain(err);
  if (errs.length === 0) return false;
  let retryable = false;
  for (const e of errs) {
    if (e.name === "AbortError") return false;
    const code = (e as { code?: unknown }).code;
    const msg = e.message.toLowerCase();
    if (NEVER_NAMES.has(e.name)) return false;
    if (typeof code === "string" && (NEVER_CODES.has(code) || code.startsWith("ERR_SSL_"))) return false;
    if (
      msg.includes("authorization violation") ||
      msg.includes("certificate") ||
      msg.includes("tls:") ||
      msg.includes("ssl routines")
    )
      return false;
    // The broker answered the handshake with a PING while it was still asking
    // its callout: it is up and busy, not refusing.
    if (msg.includes("expected 'pong'")) retryable = true;
    if (typeof code === "string" && NETWORK_CODES.has(code)) retryable = true;
    if (RETRY_NAMES.has(e.name)) retryable = true;
  }
  return retryable;
}
