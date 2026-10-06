// The conformance suite: every case in clients/conformance/cases.txt, against
// the broker that clients/conformance/nats-broker.sh starts. The `it` names are
// the case names; clients/conformance/guard.sh fails unless each one ran and
// passed. Without NATS_CLIENTS_URL the suite skips, or, with
// NATS_CLIENTS=required, fails.

import { execFileSync } from "node:child_process";
import { copyFileSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AckPolicy, DeliverPolicy, DiscardPolicy, RetentionPolicy, StorageType } from "@nats-io/jetstream";
import { headers, Match } from "@nats-io/transport-node";
import { afterEach, it as base, describe, expect } from "vitest";
import {
  accountForNamespace,
  type ConnectOptions,
  defaultConfig,
  extractTrace,
  HeaderCarrier,
  injectTrace,
  isValidPublishSubject,
  NatsClient,
  type NatsConfig,
  validateConfig,
} from "../src/index.js";

const e = process.env;
const url = e.NATS_CLIENTS_URL;
if (!url && e.NATS_CLIENTS === "required") {
  throw new Error("NATS_CLIENTS=required but NATS_CLIENTS_URL is not set: no broker");
}
const it = url ? base : base.skip;

const tlsUrl = e.NATS_CLIENTS_TLS_URL ?? "";
const tlsUrlIp = e.NATS_CLIENTS_TLS_URL_IP ?? "";
const monitor = e.NATS_CLIENTS_MONITOR ?? "";
const dir = e.NATS_CLIENTS_DIR ?? "";
const container = e.NATS_CLIENTS_CONTAINER ?? "";
const trust = e.NATS_CLIENTS_TRUST_DOMAIN ?? "";
const apiId = `spiffe://${trust}/ns/shop/sa/api`;
const workerId = `spiffe://${trust}/ns/shop/sa/worker`;
// Subjects and streams are unique per run: a broker may be reused.
const run = `r${Date.now()}${Math.floor(Math.random() * 1000)}`;
const subj = (s: string) => `conformance.${run}.${s}`;

// Each test's connections are closed when it ends, so a later broker restart is
// not a reconnect storm of earlier tests' clients against the callout.
const clients: NatsClient[] = [];
afterEach(async () => {
  await Promise.all(clients.splice(0).map((c) => c.close().catch(() => undefined)));
});

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function eventually(
  fn: () => boolean | Promise<boolean>,
  timeoutMs: number,
  what: string,
  everyMs = 100,
) {
  const end = Date.now() + timeoutMs;
  for (;;) {
    try {
      if (await fn()) return;
    } catch {
      // not yet
    }
    if (Date.now() > end) throw new Error(`timed out waiting for: ${what}`);
    await sleep(everyMs);
  }
}

/** Copies files into a per-test directory so a test can rotate them. */
function work(copies: string[] = []): string {
  const d = mkdtempSync(join(tmpdir(), "nats-ts-"));
  for (const f of copies) copyFileSync(join(dir, f), join(d, f));
  return d;
}
const CERTS = [
  "ca.crt",
  "other-ca.crt",
  "api.crt",
  "api.key",
  "worker.crt",
  "worker.key",
  "foreign.crt",
  "foreign.key",
];

function certConfig(name: string, d: string, cert: string): NatsConfig {
  return {
    ...defaultConfig(),
    url: tlsUrl,
    caFile: join(d, "ca.crt"),
    certFile: join(d, `${cert}.crt`),
    keyFile: join(d, `${cert}.key`),
    name,
    reconnectWaitMs: 200,
  };
}

function tokenConfig(name: string, tokenFile: string): NatsConfig {
  return { ...defaultConfig(), url: url ?? "", tokenFile, name, reconnectWaitMs: 200 };
}

function tokenFileWith(d: string, token: string): string {
  const f = join(d, "token");
  writeFileSync(f, token, { mode: 0o600 });
  return f;
}

async function connect(cfg: NatsConfig, opts?: ConnectOptions): Promise<NatsClient> {
  const c = await NatsClient.connect(cfg, opts);
  clients.push(c);
  return c;
}

/** A verification or authorization failure is not retried, so this is quick. */
async function connectFails(cfg: NatsConfig): Promise<Error> {
  const c = { ...cfg, connect: { ...cfg.connect, budgetMs: 2000, attempts: 3 } };
  const start = Date.now();
  let err: Error | undefined;
  try {
    const ok = await NatsClient.connect(c);
    await ok.close();
  } catch (x) {
    err = x as Error;
  }
  if (!err) throw new Error("the connection was accepted");
  expect(Date.now() - start).toBeLessThan(5000);
  return err;
}

interface ConnInfo {
  name: string;
  account: string;
  authorized_user: string;
}

async function connz(): Promise<ConnInfo[]> {
  const resp = await fetch(`${monitor}/connz?auth=true&state=open`);
  const out = (await resp.json()) as { connections?: ConnInfo[] };
  return out.connections ?? [];
}

async function waitConn(name: string, ok: (c: ConnInfo) => boolean = () => true): Promise<ConnInfo> {
  let found: ConnInfo | undefined;
  await eventually(
    async () => {
      found = (await connz()).find((c) => c.name === name);
      return found !== undefined && ok(found);
    },
    30_000,
    `connection "${name}"`,
  );
  return found as ConnInfo;
}

async function restartBroker() {
  execFileSync("docker", ["restart", "-t", "1", container], { stdio: "pipe" });
  await eventually(
    async () => (await fetch(`${monitor}/healthz`)).status === 200,
    30_000,
    "the broker came back",
  );
  // The callout reconnects on its own schedule, and a token connect before that
  // is refused after the broker's 3s callout timeout. Wait until it answers, so
  // that a test that connects with a token next is not racing it.
  const d = work();
  const tokenFile = tokenFileWith(d, "token-shop-api");
  await eventually(
    async () => {
      const probe = await NatsClient.connect({
        ...tokenConfig("callout-probe", tokenFile),
        connect: { attempts: 1, initialDelayMs: 1, maxDelayMs: 1, budgetMs: 1000 },
      });
      await probe.close();
      return true;
    },
    30_000,
    "the callout answered again",
    200,
  );
}

const healthy = async (c: NatsClient) => {
  try {
    await c.health();
    return true;
  } catch {
    return false;
  }
};

const text = (b: Uint8Array) => new TextDecoder().decode(b);

describe("conformance", () => {
  it("verify-full-connects", async () => {
    const d = work(CERTS);
    const c = await connect(certConfig("verify-full-connects", d, "api"));
    await c.health();
    const got = await waitConn("verify-full-connects");
    expect(got.authorized_user).toBe(apiId);
    expect(got.account).toBe("shop");
  });

  it("rejects-unknown-ca", async () => {
    const d = work(CERTS);
    const cfg = { ...certConfig("rejects-unknown-ca", d, "api"), caFile: join(d, "other-ca.crt") };
    const err = await connectFails(cfg);
    expect(err.message.toLowerCase(), err.message).toContain("certificate");
  });

  it("rejects-hostname-mismatch", async () => {
    const d = work(CERTS);
    // the server certificate carries only `localhost`
    const cfg = { ...certConfig("rejects-hostname-mismatch", d, "api"), url: tlsUrlIp };
    const err = await connectFails(cfg);
    expect(err.message.toLowerCase(), err.message).toContain("certificate");
  });

  it("rejects-foreign-client-cert", async () => {
    const d = work(CERTS);
    await connectFails(certConfig("rejects-foreign-client-cert", d, "foreign"));
  });

  it("refuses-unsafe-config", async () => {
    const cfg = { ...defaultConfig(), url: "tls://localhost:4222", certFile: "/x.crt", tokenFile: "/token" };
    let msg = "";
    try {
      validateConfig(cfg);
    } catch (x) {
      msg = (x as Error).message;
    }
    expect(msg).not.toBe("");
    // Every problem at once, not the first.
    for (const want of ["server CA", "go together", "two identities"]) expect(msg).toContain(want);
    expect(() =>
      validateConfig({ ...defaultConfig(), url: "http://localhost:4222", tokenFile: "/token" }),
    ).toThrow(/scheme/);
    let withCreds = "";
    try {
      validateConfig({ ...defaultConfig(), url: "nats://user:pw@localhost:4222", tokenFile: "/token" });
    } catch (x) {
      withCreds = (x as Error).message;
    }
    expect(withCreds).toContain("credentials in the URL");
    expect(withCreds).not.toContain("pw");
    // A broker that does not offer TLS is not talked to in the clear when a CA was given.
    const d = work(CERTS);
    const plain = {
      ...tokenConfig("refuses-unsafe-config", tokenFileWith(d, "token-shop-api")),
      caFile: join(d, "ca.crt"),
    };
    await connectFails(plain);
  });

  it("token-connects", async () => {
    const d = work();
    const c = await connect(tokenConfig("token-connects", tokenFileWith(d, "token-shop-api\n")));
    await c.health();
    const got = await waitConn("token-connects");
    expect(got.account).toBe("shop");
  });

  it("token-account-mapping", async () => {
    const d = work();
    await connect(tokenConfig("token-account-mapping", tokenFileWith(d, "token-other-app")));
    const got = await waitConn("token-account-mapping");
    expect(got.account).toBe(accountForNamespace("other", ["shop", "other"]));
  });

  it("rejects-unmapped-namespace", async () => {
    const d = work();
    await connectFails(tokenConfig("rejects-unmapped-namespace", tokenFileWith(d, "token-kube-system")));
    await connectFails(tokenConfig("rejects-unknown-token", tokenFileWith(d, "not-a-token")));
  });

  it("client-cert-rotation", { timeout: 90_000 }, async () => {
    const d = work(CERTS);
    const c = await connect(certConfig("client-cert-rotation", d, "api"));
    expect((await waitConn("client-cert-rotation")).authorized_user).toBe(apiId);
    // The files change under the running process, as cert-manager renews them.
    for (const ext of ["crt", "key"]) copyFileSync(join(dir, `worker.${ext}`), join(d, `api.${ext}`));
    await restartBroker();
    const got = await waitConn("client-cert-rotation", (ci) => ci.authorized_user === workerId);
    expect(got.authorized_user).toBe(workerId);
    await eventually(() => healthy(c), 20_000, "health after the restart");
  });

  it("token-file-rotation", { timeout: 90_000 }, async () => {
    const d = work();
    const tokenFile = tokenFileWith(d, "token-shop-api");
    const c = await connect(tokenConfig("token-file-rotation", tokenFile));
    const first = await waitConn("token-file-rotation");
    writeFileSync(tokenFile, "token-shop-worker");
    await restartBroker();
    const got = await waitConn(
      "token-file-rotation",
      (ci) => ci.authorized_user !== "" && ci.authorized_user !== first.authorized_user,
    );
    expect(got.authorized_user).not.toBe(first.authorized_user);
    expect(got.account).toBe("shop");
    await eventually(() => healthy(c), 20_000, "health after the restart");
  });

  it("reconnects-after-broker-restart", { timeout: 90_000 }, async () => {
    const d = work();
    const c = await connect(
      tokenConfig("reconnects-after-broker-restart", tokenFileWith(d, "token-shop-api")),
    );
    let got = 0;
    const sub = c.conn.subscribe("conformance.restart", {
      callback: () => {
        got++;
      },
    });
    await c.conn.flush();
    await restartBroker();
    // The subscription is restored by the driver; a publish after the
    // reconnect reaches it without the caller doing anything.
    await eventually(
      async () => {
        if (!(await healthy(c))) return false;
        c.conn.publish("conformance.restart", "x");
        await c.conn.flush();
        return got > 0;
      },
      30_000,
      "a message after the reconnect",
      200,
    );
    expect(sub.isClosed()).toBe(false);
  });

  it("drain-delivers-inflight", async () => {
    const d = work();
    const tokenFile = tokenFileWith(d, "token-shop-api");
    const sub = await connect(tokenConfig("drain-sub", tokenFile));
    const pub = await connect(tokenConfig("drain-pub", tokenFile));
    const n = 200;
    let got = 0;
    const s = sub.conn.subscribe("conformance.drain");
    const loop = (async () => {
      for await (const _ of s) {
        await sleep(1);
        got++;
      }
    })();
    await sub.conn.flush();
    for (let i = 0; i < n; i++) pub.conn.publish("conformance.drain", "x");
    await pub.conn.flush();
    await sub.close();
    await loop;
    expect(got, "every message delivered to the subscriber before the drain started is handled").toBe(n);
    expect(sub.conn.isClosed()).toBe(true);
  });

  it("jetstream-defaults", async () => {
    const d = work();
    const c = await connect(tokenConfig("jetstream-defaults", tokenFileWith(d, "token-shop-api")));
    const name = `DEFAULTS${Date.now()}`;
    const st = await c.ensureStream({ name, subjects: [subj("defaults.>")] });
    const cfg = st.config;
    expect(cfg.storage).toBe(StorageType.File);
    expect(cfg.retention).toBe(RetentionPolicy.Limits);
    expect(cfg.discard).toBe(DiscardPolicy.Old);
    expect(cfg.num_replicas).toBe(1);
    expect(cfg.max_age).toBe(7 * 24 * 3_600_000 * 1e6);
    expect(cfg.duplicate_window).toBe(2 * 60_000 * 1e6);
    // Idempotent, and a changed spec updates the stream.
    await c.ensureStream({ name, subjects: [subj("defaults.>")] });
    const changed = await c.ensureStream({ name, subjects: [subj("defaults.>")], maxAgeMs: 3_600_000 });
    expect(changed.config.max_age).toBe(3_600_000 * 1e6);

    const cons = await c.ensureConsumer({
      stream: name,
      durable: "worker",
      filterSubject: subj("defaults.a"),
    });
    const cc = cons.config;
    expect(cc.durable_name).toBe("worker");
    expect(cc.ack_policy).toBe(AckPolicy.Explicit);
    expect(cc.ack_wait).toBe(30_000 * 1e6);
    expect(cc.max_deliver).toBe(5);
    expect(cc.max_ack_pending).toBe(1000);
    expect(cc.deliver_policy).toBe(DeliverPolicy.All);
    expect(cc.filter_subject).toBe(subj("defaults.a"));
    await c.ensureConsumer({ stream: name, durable: "worker", filterSubject: subj("defaults.a") });
  });

  it("jetstream-publish-ack-dedup", async () => {
    const d = work();
    const c = await connect(tokenConfig("jetstream-publish-ack-dedup", tokenFileWith(d, "token-shop-api")));
    const name = `DEDUP${Date.now()}`;
    await c.ensureStream({ name, subjects: [subj("dedup.>")] });
    const first = await c.publish(subj("dedup.a"), "1", { msgId: "m-1" });
    expect(first.duplicate).toBe(false);
    const second = await c.publish(subj("dedup.a"), "1", { msgId: "m-1" });
    expect(second.duplicate).toBe(true);
    expect(second.seq).toBe(first.seq);
    // a wildcard subject is refused before it is sent
    await expect(c.publish(subj("dedup.*"), "1")).rejects.toThrow();
  });

  it("cert-identity-publish-limited", { timeout: 60_000 }, async () => {
    // The stream is made by a token client; the certificate identity may only publish.
    const d = work(CERTS);
    const admin = await connect(tokenConfig("cert-limited-admin", tokenFileWith(d, "token-shop-api")));
    const name = `LIMITED${Date.now()}`;
    await admin.ensureStream({ name, subjects: [subj("limited.>"), `forbidden.${run}.>`] });

    const c = await connect(certConfig("cert-identity-publish-limited", d, "api"));
    const ack = await c.publish(subj("limited.ok"), "x");
    expect(ack.stream).toBe(name);
    // the identity may not publish outside its subjects
    await expect(c.publish(`forbidden.${run}.no`, "x", { timeoutMs: 2000 })).rejects.toThrow();
  });

  it("propagates-trace-headers", async () => {
    const d = work();
    const c = await connect(tokenConfig("propagates-trace-headers", tokenFileWith(d, "token-shop-api")));
    const name = `TRACE${Date.now()}`;
    await c.ensureStream({ name, subjects: [subj("trace.>")] });

    const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    await c.publish(subj("trace.a"), "x", { traceparent: tp, tracestate: "k=v" });

    // What is on the wire: the lower-case keys, exactly, read by the driver without the adapter.
    const oc = await c.jetstream().consumers.get(name);
    const m = await oc.next({ expires: 5000 });
    if (!m) throw new Error("no message");
    expect(text(m.data)).toBe("x");
    const h = m.headers;
    if (!h) throw new Error("no headers");
    expect(h.keys(), "traceparent is written lower-case").toContain("traceparent");
    expect(h.keys(), "and not in the HTTP-canonical spelling").not.toContain("Traceparent");
    expect(h.has("Traceparent", Match.Exact)).toBe(false);
    expect(extractTrace(h)).toEqual({ traceparent: tp, tracestate: "k=v" });
    await m.ack();

    // A message another language wrote with the canonical spelling is still read.
    const other = headers();
    other.set("Traceparent", tp, Match.Exact);
    other.set("Tracestate", "a=b", Match.Exact);
    expect(extractTrace(other)).toEqual({ traceparent: tp, tracestate: "a=b" });
    // Setting replaces every spelling.
    injectTrace(other, "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00", "");
    expect(new HeaderCarrier(other).keys()).toEqual(["traceparent"]);
  });

  it("health-check", async () => {
    const d = work();
    const c = await connect(tokenConfig("health-check", tokenFileWith(d, "token-shop-api")));
    const start = Date.now();
    await c.health();
    expect(Date.now() - start).toBeLessThan(2000);
    await c.close();
    // a closed connection is not healthy
    await expect(c.health()).rejects.toThrow();
  });

  it("account-for-namespace-vectors", () => {
    const v = JSON.parse(
      readFileSync(new URL("../../conformance/vectors/account-for-namespace.json", import.meta.url), "utf8"),
    ) as { projectAccounts: string[]; cases: { namespace: string; account?: string; error?: boolean }[] };
    expect(v.cases.length).toBeGreaterThan(0);
    for (const c of v.cases) {
      if (c.error) expect(() => accountForNamespace(c.namespace, v.projectAccounts), c.namespace).toThrow();
      else expect(accountForNamespace(c.namespace, v.projectAccounts), c.namespace).toBe(c.account);
    }
  });

  it("subject-vectors", () => {
    const v = JSON.parse(
      readFileSync(new URL("../../conformance/vectors/subjects.json", import.meta.url), "utf8"),
    ) as { valid: string[]; invalid: string[]; tooLong: number };
    expect(v.valid.length).toBeGreaterThan(0);
    for (const s of v.valid) expect(isValidPublishSubject(s), s).toBe(true);
    for (const s of v.invalid) expect(isValidPublishSubject(s), JSON.stringify(s)).toBe(false);
    expect(isValidPublishSubject("a".repeat(v.tooLong - 1))).toBe(true);
    expect(isValidPublishSubject("a".repeat(v.tooLong))).toBe(false);
  });
});
