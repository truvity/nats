# NATS client adapters

Small libraries that connect an application to the shared broker the way this
repository's charts expect, so each product does not rewrite it: a verified
connection always, a workload certificate or a ServiceAccount token that is
read again for every (re)connect, reconnect without limit, JetStream defaults,
trace-header propagation that survives a mixed-language estate, and an orderly
drain. One contract, one conformance suite, one implementation per language.

| Language | Where | Driver | Status |
|---|---|---|---|
| Go | [`go/`](go/README.md), `github.com/truvity/nats/clients/go/natsclient` | nats.go | available |
| TypeScript | [`ts/`](ts/README.md), `@truvity/nats-client` | `@nats-io/transport-node`, `@nats-io/jetstream` | available |
| Python | `python/`, `truvity-nats-client` | nats-py | planned |
| Kotlin | `kotlin/`, `com.truvity.nats:nats-client` | jnats | planned |

## The contract

Every adapter implements this; the conformance suite proves it (see below).

### Two ways to authenticate

The broker's tenancy preset (`charts/nats-broker/presets/tenancy.yaml`) offers
two, and an adapter takes exactly one:

- **A workload certificate**: a SPIFFE identity
  (`spiffe://<trust domain>/ns/<namespace>/sa/<service account>` in the URI
  SAN) the broker maps to a user in the namespace's account. The client port
  speaks TLS and requires a certificate that chains to the broker's client CA.
  The mapped user may publish only the subjects it was given, and subscribe
  only to the reply inbox JetStream acknowledgements come back on.
- **A ServiceAccount token** the auth callout validates (TokenReview) and maps
  to the account of the token's namespace (`AccountForNamespace`). The
  connection is the broker's plaintext path, which the preset keeps for this.

### Inputs

Nothing else is read implicitly. Every adapter also takes the same values
programmatically.

| Variable | Meaning | Default |
|---|---|---|
| `NATS_URL` | the broker, **one** `nats://` or `tls://` URL; the server certificate must carry its host | required |
| `NATS_CA_FILE` | server CA file; when set the connection is TLS and trusts **only** this file (not the system store) | required with `tls://` and with a certificate |
| `NATS_CERT_FILE`, `NATS_KEY_FILE` | the workload certificate and key, both or neither | none |
| `NATS_TOKEN_FILE` | file holding the projected ServiceAccount token (audience the callout accepts) | none |
| `NATS_CLIENT_NAME` | connection name the broker shows | the program name |
| `NATS_CLIENT_CONNECT_TIMEOUT` | `5s`, `500ms` | `5s` |
| `NATS_CLIENT_RECONNECT_WAIT` | base wait between reconnects (jittered) | `1s` |
| `NATS_CLIENT_PING_INTERVAL` | | `30s` |
| `NATS_CLIENT_DRAIN_TIMEOUT` | | `10s` |
| `NATS_CLIENT_RETRY_ATTEMPTS`, `_RETRY_MAX_DELAY`, `_RETRY_BUDGET` | the **first** connection's retry, see below | `5`, `5s`, `30s` |

Exactly one identity: a certificate and key, or a token file. Credentials in
the URL are refused. Configuration that asks for anything else is an error that
lists every problem at once. There is no switch that turns verification off and
no free-form option string: a parameter passed through a string can be dropped
on the way to the driver, and a dropped CA turns a verified connection into one
that does not verify.

### TLS and credentials

- With a CA file: TLS 1.2 or later, trust is **only** that file, the server name
  is the URL's host, and a broker that does not offer TLS is not talked to in
  the clear. A client certificate is only ever sent over such a connection.
- The CA, certificate, key and token file are read **for every new physical
  connection**, never cached at start, so a cert-manager renewal or a rotated
  projected token reaches the process at its next reconnect without a restart.
  The broker closes a connection whose user JWT expires (the callout caps it at
  the token's life, at most an hour), so a token client re-reads within that.
- Keys and tokens never appear in a string form, JSON, log line or error.

### Reconnect and the first connection

- After the first connection the driver reconnects without limit, restores
  subscriptions, and keeps trying after an authorization error: a broker that
  restarts can answer before its callout is back, and a rotated token is read
  only by the next attempt.
- The **first** connection retries only what a later try can fix (the broker
  not reachable, not yet answering the handshake): exponential backoff with full
  jitter (200ms doubling to the max delay), stopping at the attempt count or the
  waiting budget and honouring cancellation. It never retries an authorization
  failure, a certificate verification failure, or a permission violation.

### Names and subjects

- **Account**: `AccountForNamespace(namespace, projectAccounts)`: a namespace in
  `projectAccounts`, `emp-<slug>`, `ci-<org>-<repo>` and exactly `ci` map to the
  account of the same name; anything else has none. Matching is exact. The
  callout applies the same rule.
- **Subjects** are not prefixed with the account; the account boundary is the
  isolation. A subject a client publishes to is concrete (no wildcards),
  dot-separated non-empty tokens of letters, digits, `-` and `_`, at most 255
  bytes, outside the reserved space (`$` prefix, `_INBOX.`). `ValidPublishSubject`
  refuses anything else before it is sent.
- The shared vectors for both rules are in
  [`conformance/vectors/`](conformance/vectors): every language, and the callout
  itself, is tested against the same files.

### JetStream

`EnsureStream` creates a stream or brings an existing one to the spec, with
defaults: file storage, limits retention, discard old, **1** replica (set it to
3 against a three-node broker), 7 days, a 2 minute de-duplication window.
`EnsureConsumer` does the same for a durable consumer: explicit
acknowledgement, 30s ack wait, 5 deliveries, 1000 unacknowledged, deliver all.
`Publish` waits for the stream's acknowledgement, takes a message ID for
de-duplication (a retried publish is idempotent within the window) and the
trace headers, and retries a missing responder (a stream still being created, a
leader election) a few times.

### Trace headers

W3C trace context travels as the message headers `traceparent` and `tracestate`.
**The header-case trap**: NATS headers are HTTP-like, and some drivers
canonicalise a key (`traceparent` becomes `Traceparent`) while others send it as
given, so a reader that looks a key up exactly misses what another language
wrote. The contract: **write the lower-case name exactly, read
case-insensitively.** Each adapter has a header helper that does both
(`HeaderCarrier` in Go also satisfies OpenTelemetry's `TextMapCarrier` without
importing it).

### Health and drain

- `Health`: a round trip to the broker through the real path (TLS,
  authentication), bounded to 2s. It suits a readiness probe.
- `Close` drains: subscriptions stop taking new messages, the ones already
  delivered are handled, pending publishes are flushed, then the connection
  closes; bounded by the drain timeout.

## Conformance

[`conformance/cases.txt`](conformance/cases.txt) is the contract's executable
form: one case name per line. Each language's suite names its tests exactly
that and runs them against a real broker with the auth callout:

- [`conformance/nats-broker.sh`](conformance/nats-broker.sh) generates, per run,
  a CA, a second unrelated CA, a server certificate that carries only the name
  `localhost`, two workload certificates (rotation) and one from the wrong
  authority, and starts a broker (image pinned by digest) with the tenancy
  shape: accounts, JetStream, TLS beside the plaintext path, certificate users
  mapped by URI SAN, and an auth callout. The callout is the **real responder**
  (`pkg/nats-auth-callout`) with the Kubernetes TokenReview replaced by a file
  of known tokens ([`conformance/callout`](conformance/callout)), so a suite
  needs a broker and not an API server.
- [`conformance/guard.sh`](conformance/guard.sh) reads the runner's machine
  output and fails unless **every** case ran and passed. A suite that skips
  because no broker was there exits 0 and reads as green; the guard is what
  makes that a red job.

```
just clients-go-conformance
just clients-ts-conformance
just clients-ts            # lint, types, unit tests, build; no broker
```

The conformance tests skip without `NATS_CLIENTS_URL` so that `go test ./...`
and `npm test` stay local-friendly; `NATS_CLIENTS=required` (set by the
recipes) turns a missing broker into a failure.

Certificate and token rotation are proven without restarting the client: the
files change under the running process, the broker is restarted to drop the
connection, and the broker's monitor (`/connz`) shows the client back as the new
identity.

## Releasing

One `v*` tag stamps the charts, the image and the Go module. The Go adapter is
part of the `github.com/truvity/nats` module, so it needs nothing more. The
TypeScript package is published to GitHub Packages by a second job in
`release.yaml` (after the release job succeeds), at the tag without its `v`; CI
prints `npm pack --dry-run` so a PR shows what would ship.

```
go get github.com/truvity/nats@v1.7.0     # Go: the tag is the module version
```

For `@truvity/nats-client`, point the scope at GitHub Packages in `.npmrc`. The
registry answers 401 to anonymous requests **even for a public package**, so a
token with `read:packages` is always required:

```
@truvity:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_PACKAGES_TOKEN}
```

then `npm install @truvity/nats-client`. In GitHub Actions the token is
`${{ github.token }}` with `packages: read`.
