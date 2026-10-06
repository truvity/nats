import { errors } from "@nats-io/transport-node";
import { describe, expect, it } from "vitest";
import { isRetryable, type RetryPolicy, retry } from "../src/index.js";

const fast: RetryPolicy = { attempts: 4, initialDelayMs: 1, maxDelayMs: 4, budgetMs: 1000 };
const withCode = (code: string, message = "boom") => Object.assign(new Error(message), { code });

describe("isRetryable", () => {
  const cases: [string, unknown, boolean][] = [
    ["not an error", "x", false],
    ["connection error", new errors.ConnectionError("connection refused"), true],
    ["timeout", new errors.TimeoutError(), true],
    ["refused (cause)", new Error("dial", { cause: withCode("ECONNREFUSED") }), true],
    ["reset", withCode("ECONNRESET"), true],
    ["handshake raced the callout", new Error("expected 'PONG', got 'PING'"), true],
    ["authorization", new errors.AuthorizationError("nope"), false],
    ["authorization text", new Error("Authorization Violation"), false],
    ["expired", new errors.UserAuthenticationExpiredError("x"), false],
    ["permission", new errors.PermissionViolationError("x", "publish", "a.b"), false],
    [
      "unknown authority",
      withCode("SELF_SIGNED_CERT_IN_CHAIN", "self-signed certificate in certificate chain"),
      false,
    ],
    ["hostname", withCode("ERR_TLS_CERT_ALTNAME_INVALID", "Hostname/IP does not match"), false],
    [
      "a refusal wrapping a certificate failure",
      new errors.ConnectionError("x", { cause: withCode("CERT_HAS_EXPIRED") }),
      false,
    ],
    ["plain", new Error("boom"), false],
  ];
  for (const [name, err, want] of cases) {
    it(name, () => expect(isRetryable(err)).toBe(want));
  }
});

describe("retry", () => {
  it("stops at attempts", async () => {
    let n = 0;
    await expect(
      retry(fast, async () => {
        n++;
        throw withCode("ECONNRESET");
      }),
    ).rejects.toThrow("boom");
    expect(n).toBe(4);
  });

  it("does not retry a permanent error", async () => {
    let n = 0;
    await expect(
      retry(fast, async () => {
        n++;
        throw new errors.AuthorizationError("x");
      }),
    ).rejects.toThrow();
    expect(n).toBe(1);
  });

  it("succeeds after transient failures", async () => {
    let n = 0;
    await retry(fast, async () => {
      if (++n < 3) throw new errors.ConnectionError("down");
    });
    expect(n).toBe(3);
  });

  it("honours cancellation", async () => {
    const ac = new AbortController();
    let n = 0;
    await expect(
      retry(
        fast,
        async () => {
          n++;
          ac.abort();
          throw withCode("ECONNRESET");
        },
        ac.signal,
      ),
    ).rejects.toThrow();
    expect(n).toBe(1);
  });

  it("rejects a bad policy", async () => {
    await expect(
      retry({ attempts: 0, initialDelayMs: 0, maxDelayMs: 0, budgetMs: 0 }, async () => 1),
    ).rejects.toThrow();
  });
});
