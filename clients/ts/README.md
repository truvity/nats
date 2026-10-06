# @truvity/nats-client (TypeScript)

The TypeScript adapter of the NATS client contract, on the official
`@nats-io/transport-node` and `@nats-io/jetstream` (nats.js v3). ESM, Node 20
or later. Published to GitHub Packages. It behaves like the Go adapter
(`clients/go/natsclient`) and is held to the same conformance cases.

## Install

GitHub Packages answers 401 to anonymous installs even for a public package,
so a token with `read:packages` is always needed. In `.npmrc`:

```
@truvity:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_PACKAGES_TOKEN}
```

```
npm install @truvity/nats-client
```

Locally the token can be `$(gh auth token)` (after
`gh auth refresh -s read:packages`); in GitHub Actions use
`${{ github.token }}` with `packages: read`. Yarn 4: set
`npmScopes.truvity.npmRegistryServer` to `https://npm.pkg.github.com` and
`npmAuthToken` in `.yarnrc.yml`.

## Use

```ts
import { configFromEnv, NatsClient } from "@truvity/nats-client";

const nc = await NatsClient.connect(configFromEnv()); // NATS_URL, NATS_CA_FILE, ... ; retries the first connection

await nc.ensureStream({ name: "ORDERS", subjects: ["orders.>"] });
const ack = await nc.publish("orders.created", JSON.stringify({ id: 1 }), {
  msgId: "order-1",        // a retried publish is de-duplicated
  traceparent,             // written as the lower-case trace headers
});

await nc.ensureConsumer({ stream: "ORDERS", durable: "worker", filterSubject: "orders.created" });
const consumer = await nc.jetstream().consumers.get("ORDERS", "worker");
for await (const m of await consumer.consume()) {
  // handle m, then
  m.ack();
}

app.get("/readyz", async (_req, res) => {
  await nc.health().then(() => res.sendStatus(200), () => res.sendStatus(503)); // bounded to 2s
});

process.on("SIGTERM", () => nc.close()); // drains: delivered messages are handled, publishes flushed
```

`nc.conn` is the driver's `NatsConnection` for subscriptions and requests;
`nc.jetstream()` and `await nc.jetstreamManager()` are the driver's JetStream
clients. Do not close `nc.conn` yourself; use `nc.close()`.

## Environment

| Variable | Meaning |
| --- | --- |
| `NATS_URL` | one `nats://` or `tls://` URL (required) |
| `NATS_CA_FILE` | server CA; makes the connection TLS and is the only trust root (required with `tls://` or a client certificate) |
| `NATS_CERT_FILE`, `NATS_KEY_FILE` | workload certificate and key, both or neither |
| `NATS_TOKEN_FILE` | projected ServiceAccount token; not together with a certificate |
| `NATS_CLIENT_NAME` | connection name (default: the script name) |
| `NATS_CLIENT_CONNECT_TIMEOUT` | default `5s` |
| `NATS_CLIENT_RECONNECT_WAIT` | default `1s` |
| `NATS_CLIENT_PING_INTERVAL` | default `30s` |
| `NATS_CLIENT_DRAIN_TIMEOUT` | default `10s` |
| `NATS_CLIENT_RETRY_ATTEMPTS`, `_RETRY_MAX_DELAY`, `_RETRY_BUDGET` | first-connection retry, default `5`, `5s`, `30s` |

Durations are Go style: `500ms`, `5s`, `1m30s`. Exactly one credential is
required: a certificate and key, or a token file.

## What it does

- `validateConfig` (called by `configFromEnv` and `NatsClient.connect`) throws
  one `ConfigError` that lists every problem. There is no switch that turns
  verification off. URLs with credentials are refused, and `redactConfig` /
  `redactUrl` carry no secret.
- The server is verified against the CA file only (not the system store), with
  the URL's host as the name. The CA, certificate, key and token files are
  read again for every connection and reconnect, so rotated files are used
  without a restart. The driver reconnects without limit.
- The first connection is retried (exponential backoff with full jitter) while
  the broker is unreachable; a certificate or authorization failure is thrown
  at once.
- `ensureStream` / `ensureConsumer` apply the estate defaults: file storage,
  limits retention, discard old, 1 replica, max age 7d, duplicate window 2m;
  explicit ack, ack wait 30s, max deliver 5, max ack pending 1000, deliver all.
- `publish` waits for the stream's acknowledgement, retries a missing
  responder three times, validates the subject (`validatePublishSubject`) and
  de-duplicates by `msgId`.
- `accountForNamespace` is the broker's namespace-to-account rule, tested
  against the vectors shared with the callout and the other languages.

## The header trap

NATS headers are HTTP-like and nats.js can canonicalise a key
(`traceparent` becomes `Traceparent`) when `Match.CanonicalMIME` is used. A
reader that looks a key up exactly then misses what another language wrote.
The contract: write `traceparent` and `tracestate` lower case exactly, read
case-insensitively. `injectTrace` / `extractTrace` / `HeaderCarrier` do that
(`Match.Exact` to write, `Match.IgnoreCase` to read); `publish` uses them. Use
`HeaderCarrier` as the carrier for an OpenTelemetry propagator:

```ts
propagation.inject(context.active(), new HeaderCarrier(h), {
  set: (c, k, v) => c.set(k, v),
});
```

## Differences from Go

- Names are camelCase and durations are milliseconds (`reconnectWaitMs`).
- `ensureStream` / `ensureConsumer` return the stream's / consumer's info
  (with `config`), not a handle; use `nc.jetstream()` for handles.
- Calls take no `context`: `NatsClient.connect` takes an `AbortSignal` in its
  options, `health()` is bounded to 2s, `close()` by `drainTimeoutMs`.
- `accountForNamespace` and `validatePublishSubject` throw instead of
  returning an error; `isValidPublishSubject` is the boolean form.
- The logger is a plain `{ info, warn, error }` object (`ConnectOptions.logger`).
- Implementation note: nats.js reports a refused handshake (an unknown client
  certificate, a missing credential) only after the connect timeout when its
  reconnect is on. The first connection is therefore made with reconnect off
  and the driver's flag is switched on once connected (see `enableReconnect`),
  so a bad certificate fails at once and by name. Hosts are dialled by name
  (`resolve: false`) so a refused `::1` does not hide the real error.

## Develop

```
cd clients/ts
npm ci && npm run lint && npm run typecheck && npm test && npm run build
```

The conformance suite (`test/conformance.test.ts`) needs the broker:

```
eval "$(clients/conformance/nats-broker.sh up)"
NATS_CLIENTS=required npm run test:conformance
clients/conformance/nats-broker.sh down
```

Without `NATS_CLIENTS_URL` the suite skips; with `NATS_CLIENTS=required` it
fails instead. `clients/conformance/guard.sh ts <vitest --reporter=json file>`
fails unless every case in `cases.txt` ran and passed.
