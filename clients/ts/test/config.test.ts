import { describe, expect, it } from "vitest";
import {
  ConfigError,
  configFromEnv,
  defaultConfig,
  parseDuration,
  redactConfig,
  redactUrl,
  validateConfig,
} from "../src/index.js";

const problemsOf = (fn: () => unknown): string => {
  try {
    fn();
  } catch (e) {
    expect(e).toBeInstanceOf(ConfigError);
    return (e as Error).message;
  }
  throw new Error("expected a ConfigError");
};

describe("configFromEnv", () => {
  it("reads a token configuration", () => {
    const c = configFromEnv({ NATS_URL: "nats://nats.example:4222", NATS_TOKEN_FILE: "/var/run/token" });
    expect(c.tokenFile).toBe("/var/run/token");
    expect(c.caFile).toBeUndefined();
    expect(c.connect).toEqual(defaultConfig().connect);
    expect(c.name).not.toBe("");
  });

  it("reads a certificate configuration and Go-style durations", () => {
    const c = configFromEnv({
      NATS_URL: "tls://nats.example:4222",
      NATS_CA_FILE: "/ca",
      NATS_CERT_FILE: "/crt",
      NATS_KEY_FILE: "/key",
      NATS_CLIENT_NAME: "svc",
      NATS_CLIENT_RECONNECT_WAIT: "250ms",
      NATS_CLIENT_DRAIN_TIMEOUT: "1m30s",
      NATS_CLIENT_RETRY_ATTEMPTS: "7",
    });
    expect(c.name).toBe("svc");
    expect(c.reconnectWaitMs).toBe(250);
    expect(c.drainTimeoutMs).toBe(90_000);
    expect(c.connect.attempts).toBe(7);
  });

  it("lists every problem", () => {
    expect(
      problemsOf(() => configFromEnv({ NATS_URL: "ftp://a,b", NATS_CLIENT_PING_INTERVAL: "soon" })),
    ).toContain("PING_INTERVAL");
    expect(problemsOf(() => configFromEnv({ NATS_URL: "tls://a:4222", NATS_TOKEN_FILE: "/t" }))).toContain(
      "server CA",
    );
    expect(problemsOf(() => configFromEnv({ NATS_URL: "nats://a:4222" }))).toContain(
      "credential is required",
    );
    expect(
      problemsOf(() =>
        configFromEnv({ NATS_URL: "nats://a:4222", NATS_CERT_FILE: "/c", NATS_KEY_FILE: "/k" }),
      ),
    ).toContain("server CA");
  });

  it("reports several problems in one error", () => {
    const c = { ...defaultConfig(), url: "tls://localhost:4222", certFile: "/x.crt", tokenFile: "/token" };
    const msg = problemsOf(() => validateConfig(c));
    for (const want of ["server CA", "go together", "two identities"]) expect(msg).toContain(want);
  });

  it("refuses a URL with credentials without printing them", () => {
    const c = { ...defaultConfig(), url: "nats://user:pw@localhost:4222", tokenFile: "/token" };
    const msg = problemsOf(() => validateConfig(c));
    expect(msg).toContain("credentials in the URL");
    expect(msg).not.toContain("pw");
  });

  it("refuses another scheme", () => {
    const c = { ...defaultConfig(), url: "http://localhost:4222", tokenFile: "/token" };
    expect(problemsOf(() => validateConfig(c))).toContain("scheme");
  });

  it("never prints a credential", () => {
    const c = {
      ...defaultConfig(),
      url: "nats://user:s3cret@nats.example:4222",
      tokenFile: "/var/run/token",
    };
    expect(JSON.stringify(redactConfig(c))).not.toContain("s3cret");
    expect(redactUrl(c.url)).not.toContain("s3cret");
  });
});

describe("parseDuration", () => {
  it("parses Go durations", () => {
    expect(parseDuration("5s")).toBe(5000);
    expect(parseDuration("500ms")).toBe(500);
    expect(parseDuration("1h2m3s")).toBe(3_723_000);
    expect(parseDuration("1.5s")).toBe(1500);
    expect(parseDuration("0")).toBe(0);
  });
  it("rejects the rest", () => {
    for (const bad of ["", "5", "s", "1d", "soon", "-1s"]) expect(() => parseDuration(bad)).toThrow();
  });
});
