import { readFileSync } from "node:fs";
import {
  type ConsumerInfo,
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
  type PubAck,
  type StreamInfo,
} from "@nats-io/jetstream";
import {
  type NatsConnection,
  type NodeConnectionOptions,
  connect as natsConnect,
  type TlsOptions,
} from "@nats-io/transport-node";
import { type NatsConfig, redactUrl, validateConfig } from "./config.js";
import {
  type ConsumerSpec,
  ensureConsumer,
  ensureStream,
  type PublishOptions,
  publish,
  type StreamSpec,
} from "./jetstream.js";
import { retry } from "./retry.js";

/** Bounds {@link NatsClient.health}, ms. */
export const HEALTH_TIMEOUT_MS = 2_000;

/** Where disconnects, reconnects and asynchronous errors are reported. Nothing it is given contains a credential. */
export interface Logger {
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
}

export interface ConnectOptions {
  logger?: Logger;
  /** Aborts the retry of the first connection. */
  signal?: AbortSignal;
}

const quiet: Logger = { info() {}, warn() {}, error() {} };

/**
 * One connection to the broker. It reconnects without limit; the credential
 * files are read again for every reconnect.
 */
export class NatsClient {
  private jsm?: JetStreamManager;
  private readonly js: JetStreamClient;

  private constructor(
    readonly config: NatsConfig,
    /** The driver's connection, for what the adapter does not wrap (subscriptions, requests). Do not close it; use {@link NatsClient.close}. */
    readonly conn: NatsConnection,
  ) {
    this.js = jetstream(conn);
  }

  /**
   * Validates `cfg` and opens the connection. The first connection is retried
   * (see `cfg.connect`) while the broker is not reachable; a failed
   * verification or authorization is thrown at once.
   */
  static async connect(cfg: NatsConfig, opts: ConnectOptions = {}): Promise<NatsClient> {
    validateConfig(cfg);
    const logger = opts.logger ?? quiet;
    const nopts = driverOptions(cfg, logger);
    let nc: NatsConnection;
    try {
      nc = await retry(cfg.connect, () => natsConnect({ ...nopts, reconnect: false }), opts.signal);
      enableReconnect(nc);
    } catch (err) {
      throw new Error(`natsclient: connect ${redactUrl(cfg.url)}: ${(err as Error).message}`, { cause: err });
    }
    if (cfg.caFile && encrypted(nc) === false) {
      await nc.close();
      throw new Error(
        `natsclient: connect ${redactUrl(cfg.url)}: the broker did not offer TLS but a CA file was given`,
      );
    }
    void watchStatus(nc, logger);
    return new NatsClient(cfg, nc);
  }

  /** The driver's JetStream client (publish, consume). */
  jetstream(): JetStreamClient {
    return this.js;
  }

  /** The driver's JetStream manager, created on first use. */
  async jetstreamManager(): Promise<JetStreamManager> {
    // checkAPI off: a publish-only identity may not call $JS.API.INFO.
    this.jsm ??= await jetstreamManager(this.conn, { checkAPI: false });
    return this.jsm;
  }

  /** Creates the stream or brings an existing one to the spec. Returns the stream's info (the Go adapter returns a handle). */
  ensureStream(spec: StreamSpec): Promise<StreamInfo> {
    return ensureStream(this, spec);
  }

  /** Creates the durable consumer or brings an existing one to the spec. Returns the consumer's info. */
  ensureConsumer(spec: ConsumerSpec): Promise<ConsumerInfo> {
    return ensureConsumer(this, spec);
  }

  /**
   * Sends `data` to a JetStream subject and waits for the stream's
   * acknowledgement, retrying a missing responder (a stream still being
   * created or a leader election) a few times.
   */
  publish(subject: string, data: Uint8Array | string, opts?: PublishOptions): Promise<PubAck> {
    return publish(this, subject, data, opts);
  }

  /**
   * A round trip to the broker through the real path (TLS, authentication),
   * bounded to 2s. Rejects when the connection is closed, draining or does not
   * answer. It suits a readiness probe.
   */
  async health(): Promise<void> {
    if (this.conn.isClosed()) throw new Error("natsclient: not connected (closed)");
    if (this.conn.isDraining()) throw new Error("natsclient: not connected (draining)");
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`natsclient: health check timed out after ${HEALTH_TIMEOUT_MS}ms`)),
        HEALTH_TIMEOUT_MS,
      );
    });
    try {
      await Promise.race([this.conn.flush(), timeout]);
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * Drains the connection: subscriptions stop taking new messages, the ones
   * already delivered are handled, pending publishes are flushed, then the
   * connection closes. Resolves when that is done; after `drainTimeoutMs` the
   * connection is closed and this rejects.
   */
  async close(): Promise<void> {
    if (this.conn.isClosed()) return;
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<"timeout">((resolve) => {
      timer = setTimeout(() => resolve("timeout"), this.config.drainTimeoutMs);
    });
    try {
      const r = await Promise.race([this.conn.drain().then(() => "done" as const), timeout]);
      if (r === "timeout") {
        await this.conn.close();
        throw new Error(
          `natsclient: drain did not finish in ${this.config.drainTimeoutMs}ms; connection closed`,
        );
      }
    } catch (err) {
      if (this.conn.isClosed()) return;
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }
}

/**
 * nats.js reports a handshake the broker refuses (an unknown client certificate,
 * a missing credential) only after the connect timeout when reconnect is on,
 * and as the real TLS or authorization error at once when it is off. The first
 * connection is therefore made with reconnect off, so a bad certificate or token
 * fails fast and by name instead of as a "timeout" that would also be retried;
 * once connected the driver's flag is switched on, which the driver reads at
 * every disconnect. The reconnect conformance cases keep this honest.
 */
function enableReconnect(nc: NatsConnection): void {
  const opts = (nc as unknown as { protocol?: { options?: { reconnect?: boolean } } }).protocol?.options;
  if (!opts) {
    void nc.close();
    throw new Error("natsclient: unsupported @nats-io/transport-node version: cannot enable reconnect");
  }
  opts.reconnect = true;
}

function encrypted(nc: NatsConnection): boolean | undefined {
  // Not part of the public API; best effort, and only an explicit `false` is acted on.
  const sock = (nc as unknown as { protocol?: { transport?: { socket?: { encrypted?: boolean } } } }).protocol
    ?.transport?.socket;
  return sock?.encrypted;
}

/** The driver options for `cfg`: verify-full against the CA file, credentials read for every connection. */
export function driverOptions(cfg: NatsConfig, logger: Logger = quiet): NodeConnectionOptions {
  const o: NodeConnectionOptions = {
    servers: cfg.url,
    name: cfg.name,
    timeout: cfg.connectTimeoutMs,
    maxReconnectAttempts: -1,
    reconnectTimeWait: cfg.reconnectWaitMs,
    reconnectJitter: cfg.reconnectWaitMs / 2,
    reconnectJitterTLS: cfg.reconnectWaitMs / 2,
    pingInterval: cfg.pingIntervalMs,
    // A broker restart can answer a reconnect before its callout is back, and a
    // rotated token is only read on the next attempt: keep trying instead of
    // giving up on the second identical authorization error.
    ignoreAuthErrorAbort: true,
    noAsyncTraces: true,
    // Dial the host name as given and let Node pick the address family. The
    // driver's own resolution tries every address and reports the last
    // failure, which hides a certificate error behind a refused "::1".
    resolve: false,
  };
  if (cfg.caFile) {
    // The driver reads caFile, certFile and keyFile again for every connection
    // (and every reconnect). Verification is the driver's: rejectUnauthorized,
    // servername = the URL's host, and `ca` replaces the system store.
    o.tls = {
      caFile: cfg.caFile,
      ...(cfg.certFile ? { certFile: cfg.certFile } : {}),
      ...(cfg.keyFile ? { keyFile: cfg.keyFile } : {}),
      rejectUnauthorized: true,
      // Without it an IP URL is checked against "localhost".
      servername: new URL(cfg.url).hostname.replace(/^\[|\]$/g, ""),
    } as TlsOptions;
  } else {
    // Left unset the driver upgrades to TLS against the system store whenever the server offers it.
    o.tls = null;
  }
  if (cfg.tokenFile) {
    const file = cfg.tokenFile;
    o.authenticator = () => {
      try {
        return { auth_token: readFileSync(file, "utf8").trim() };
      } catch (err) {
        logger.error("nats token file unreadable", { file, error: (err as Error).message });
        return { auth_token: "" };
      }
    };
  }
  return o;
}

async function watchStatus(nc: NatsConnection, logger: Logger): Promise<void> {
  try {
    for await (const s of nc.status()) {
      switch (s.type) {
        case "disconnect":
          logger.warn("nats disconnected", { server: s.server });
          break;
        case "reconnect":
          logger.info("nats reconnected", { server: s.server });
          break;
        case "error":
          logger.error("nats error", { error: s.error.message });
          break;
        default:
          break;
      }
    }
  } catch {
    // the status iterator ends with the connection
  }
}
