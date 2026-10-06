# truvity-nats-client (Python)

The Python adapter of the [NATS client contract](../README.md), on
[nats-py](https://github.com/nats-io/nats.py) (asyncio). Python 3.10 or later.
Import name: `truvity_nats`. It behaves like the Go adapter
(`clients/go/natsclient`) and is held to the same conformance cases.

```python
import asyncio
from truvity_nats import ConsumerSpec, NatsClient, NatsConfig, StreamSpec


async def main() -> None:
    # NATS_URL, NATS_CA_FILE, ...; retries the first connection
    nc = await NatsClient.connect(NatsConfig.from_env())

    await nc.ensure_stream(StreamSpec(name="ORDERS", subjects=["orders.>"]))
    spec = ConsumerSpec(stream="ORDERS", durable="billing", filter_subject="orders.created")
    await nc.ensure_consumer(spec)

    # waits for the stream's acknowledgement; msg_id de-duplicates a retried publish
    ack = await nc.publish("orders.created", b"{}", msg_id="order-42", traceparent="00-...-...-01")

    sub = await nc.js.pull_subscribe("orders.created", durable="billing")  # nc.js: nats-py's JetStreamContext
    for m in await sub.fetch(10, timeout=2):
        await m.ack()

    await nc.health()  # a readiness probe; bounded to 2s, raises NatsClientError
    await nc.close()  # drains: delivered messages are handled, publishes flushed


asyncio.run(main())
```

`nc.conn` is nats-py's `Client` for subscriptions and requests and `nc.js` its
`JetStreamContext`. Do not close `nc.conn` yourself; use `nc.close()`.

## Install

Released as a **wheel and an sdist attached to the GitHub release** of the tag,
not on a package index: GitHub Packages has no Python registry, and release
assets of a public repository need no token.

```
pip install "truvity-nats-client @ https://github.com/truvity/nats/releases/download/v1.7.0/truvity_nats_client-1.7.0-py3-none-any.whl"
# uv:
uv add "truvity-nats-client @ https://github.com/truvity/nats/releases/download/v1.7.0/truvity_nats_client-1.7.0-py3-none-any.whl"
```

The version is the repository's `v*` tag without the `v`, shared with the
charts, the Go module and the other clients. Pin the URL.

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

Durations are Go style (`500ms`, `5s`, `1m30s`). Exactly one credential is
required: a certificate and key, or a token file. `NatsConfig.validate()` (run
by `from_env` and `connect`) raises one `ConfigError` listing every problem;
there is no switch that turns verification off, and `repr()` of the
configuration carries no secret (credentials in a URL are refused and redacted).

## What it does

- The server is verified against the CA file only (not the system store),
  TLS 1.2 or later, host name = the URL's host. A broker that does not offer TLS
  is not talked to when a CA was given: nothing, token included, is sent in the
  clear.
- The CA, certificate, key and token files are read again for **every** new
  connection and reconnect, so rotated files are used without a restart. The
  driver reconnects without limit, restores subscriptions, and keeps trying
  after an authorization error.
- The first connection is retried (exponential backoff with full jitter) only
  while the broker is unreachable; a certificate or authorization failure is
  raised at once, wrapped in `NatsClientError` with the cause chained.
- `ensure_stream` / `ensure_consumer` apply the estate defaults: file storage,
  limits retention, discard old, 1 replica, max age 7d, duplicate window 2m;
  explicit ack, ack wait 30s, max deliver 5, max ack pending 1000, deliver all.
- `publish` waits for the acknowledgement, validates the subject, retries a
  missing responder three times, de-duplicates by `msg_id` (`Nats-Msg-Id`) and
  writes the trace headers.
- `account_for_namespace` and `valid_publish_subject` are tested against the
  vectors shared with the callout and the other languages.

## The header trap

`inject_trace` writes `traceparent` / `tracestate` lower case exactly and
replaces any other spelling; `extract_trace` reads them case-insensitively;
`HeaderCarrier` wraps a header dict (nats-py's `Msg.headers`) the same way and
is a `MutableMapping`, so an OpenTelemetry propagator takes it as a carrier
without this package importing OpenTelemetry. `publish` uses them.

## How the files are re-read on every reconnect

nats-py has no such feature: it takes one `ssl.SSLContext` at `connect()` and
reuses it for every reconnect. This adapter subclasses `nats.aio.client.Client`
(`truvity_nats.client._Connection`) and overrides three things:

1. **`ssl_context`**, nats-py's own (public) property, which the driver reads for
   every TLS handshake. The override builds a new context each time:
   `ssl.create_default_context(cafile=<the CA file>)` (that file and nothing
   else), `CERT_REQUIRED`, `check_hostname` on, TLS 1.2 minimum, and
   `load_cert_chain` of the current certificate and key. The host name checked
   is the URL's (`tls_hostname`).
2. **`_process_info`** (private). It runs on the broker's first INFO, before
   anything is sent. With a CA file it refuses a broker that offers no TLS, and
   it starts the upgrade for a broker that only *offers* it (`tls_available`,
   the tenancy preset's `allow_non_tls`): nats-py upgrades only on
   `tls_required`, so without this the client would stay in the clear on that
   port.
3. **`_process_err`** (private). On an established connection nats-py closes
   for good on `Authorization Violation`; the broker sends the same error when
   the callout's user JWT expires. The override turns it into a reconnect, which
   reads the token again.

The token is nats-py's `token=` callable, which the driver calls for every
CONNECT. Pace and jitter come from `reconnect_to_server_handler` (the first
attempt after a disconnect is immediate; later ones wait `reconnect_wait` plus
up to 50% jitter).

Because two hooks are private, the dependency is `nats-py>=2.16.0,<2.17`, an
import-time check fails loudly if a hook has moved, and the conformance cases
(certificate rotation, token rotation, restart) are what proves it works. A new
nats-py minor needs the suite run before the bound is widened.

## Differences from Go

- **asyncio.** Every call that touches the broker is a coroutine. Durations in
  `NatsConfig` are seconds (floats); the environment uses `500ms`, `5s`.
- **No `context`.** Cancel the task (`asyncio.wait_for`, a task group) instead.
  `connect` is retried only on connection-class errors and honours cancellation;
  `health()` is bounded to 2s; `close()` to `drain_timeout` (plus 2s for the
  flush and close) and raises `NatsClientError` when the drain does not finish
  in time.
- **Return types** are nats-py's: `ensure_stream` returns a `StreamInfo`,
  `ensure_consumer` a `ConsumerInfo` (with `.config`), `publish` a `PubAck`
  (`.stream`, `.seq`, `.duplicate`). Use `nc.js` for handles.
- **Errors.** Adapter errors are `ConfigError` (a `ValueError`),
  `NatsClientError`, `CredentialError` (an unreadable or unusable credential
  file), `SubjectError`, `NamespaceError`. `account_for_namespace` and
  `valid_publish_subject` raise; `is_valid_publish_subject` is the boolean form.
- **Logging** is the standard library's (`logging.getLogger("truvity_nats")`, or
  `NatsClient.connect(cfg, logger=...)`). Nothing logged contains a credential.
- **Headers** are nats-py's `dict[str, str]` (one value per name); the Go
  `[]string` form is not offered.
- **Blocking reads.** The credential files are small and read synchronously on
  the event loop at each (re)connect.
- **Strict X.509 profile off.** Python 3.13 adds `VERIFY_X509_STRICT` to default
  contexts, which rejects a CA certificate without a `keyUsage` extension. The
  adapter clears that one flag; chain, name, expiry and trust-only-the-CA-file
  checks stay on, as they are in Go and Node.
- **First connection.** nats-py's `connect()` retries every error forever when
  reconnect is allowed, and cycles through a refused address without limit
  otherwise, which would also retry a bad certificate. The adapter therefore
  connects with reconnect off (and one extra driver try per adapter attempt,
  without a wait) and turns reconnect on once connected; the real error of the
  last try is what is raised.
- **JetStream.** `ensure_stream` tries an update first and creates the stream on
  "not found", as the Go adapter does.

## Develop

```
cd clients/python
uv sync --locked && uv run ruff check . && uv run ruff format --check . && uv run mypy src tests && uv run pytest
just clients-python               # from the repository root: the same, plus a build
just clients-python-conformance   # needs docker
```

The conformance suite (`tests/test_conformance.py`, one test per case in
`conformance/cases.txt`, named `test_<case with _ for ->`) needs the broker:

```
eval "$(clients/conformance/nats-broker.sh up)"
NATS_CLIENTS=required uv run pytest tests/test_conformance.py --junitxml=report.xml
clients/conformance/guard.sh python report.xml
clients/conformance/nats-broker.sh down
```

Without `NATS_CLIENTS_URL` the suite skips; with `NATS_CLIENTS=required` it
fails instead. `guard.sh python <junit xml>` fails unless every case in
`cases.txt` ran and passed. The unit tests (config, retry, headers, vectors, the
driver glue) need no broker.
