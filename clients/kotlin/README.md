# com.truvity.nats:nats-client (Kotlin / JVM)

The Kotlin adapter of the [client contract](../README.md), on the official
[jnats](https://github.com/nats-io/nats.java) (`io.nats:jnats`). Java 21 or
later; usable from Java as well as Kotlin. Blocking API. It behaves like the Go
adapter (`clients/go/natsclient`) and is held to the same conformance cases.

```kotlin
import com.truvity.nats.*

val nc = NatsClient.connect(NatsConfig.fromEnv())   // NATS_URL, NATS_CA_FILE, ... ; retries the first connection

nc.ensureStream(StreamSpec("ORDERS", listOf("orders.>")))
val ack = nc.publish("orders.created", """{"id":1}""".toByteArray(),
    PublishOptions(msgId = "order-1", traceparent = traceparent))   // de-duplicated; lower-case trace headers

nc.ensureConsumer(ConsumerSpec("ORDERS", "worker", filterSubject = "orders.created"))
// the driver's own API for subscriptions, requests, consumers:
nc.connection.createDispatcher { msg -> /* handle */ }.subscribe("some.subject")
val consumer = nc.connection.getConsumerContext("ORDERS", "worker")

fun ready(): Boolean = runCatching { nc.health() }.isSuccess   // bounded to 2s

Runtime.getRuntime().addShutdownHook(Thread { nc.close() })     // drains
```

`nc.connection` is jnats's `Connection`, `nc.jetStream()` and
`nc.jetStreamManagement()` its JetStream clients. Do not close `nc.connection`
yourself; use `nc.close()`.

## Install

The package is on **GitHub Packages**. GitHub Packages asks for a token even
when the repository is public, so a consumer needs a token in
`~/.m2/settings.xml` (a classic personal access token with `read:packages`;
in GitHub Actions, `GITHUB_TOKEN` with `packages: read`):

```xml
<settings>
  <servers>
    <server>
      <id>github-truvity-nats</id>
      <username>YOUR_GITHUB_LOGIN</username>
      <password>${env.GITHUB_TOKEN}</password>
    </server>
  </servers>
</settings>
```

and the repository in the `pom.xml` (the `id` must match the server id):

```xml
<repositories>
  <repository>
    <id>github-truvity-nats</id>
    <url>https://maven.pkg.github.com/truvity/nats</url>
  </repository>
</repositories>

<dependency>
  <groupId>com.truvity.nats</groupId>
  <artifactId>nats-client</artifactId>
  <version>1.7.0</version> <!-- the repository tag, without the v -->
</dependency>
```

The version is the repository's `v*` tag, shared with the charts, the image and
the other clients. The sources jar is published beside the jar.

## Environment

The same variables as the other adapters (see the [contract](../README.md#inputs)):
`NATS_URL`, `NATS_CA_FILE`, `NATS_CERT_FILE`, `NATS_KEY_FILE`, `NATS_TOKEN_FILE`,
`NATS_CLIENT_NAME`, `NATS_CLIENT_CONNECT_TIMEOUT`, `NATS_CLIENT_RECONNECT_WAIT`,
`NATS_CLIENT_PING_INTERVAL`, `NATS_CLIENT_DRAIN_TIMEOUT`,
`NATS_CLIENT_RETRY_ATTEMPTS`, `_RETRY_MAX_DELAY`, `_RETRY_BUDGET`. Durations are
Go style: `500ms`, `5s`, `1m30s`. `NatsConfig.fromEnv()` validates and throws one
`ConfigError` listing every problem; `NatsConfig(...)` plus `validate()` does the
same for programmatic use. There is no switch that turns verification off, and
`toString()` of the configuration carries no credential.

## What it does

- The server is verified against the CA file only (never the JVM's trust store),
  the chain by the JDK's PKIX trust manager and the name against the URL's host
  (endpoint identification `HTTPS`, which also sets SNI). TLS 1.2 or later. A
  `nats://` URL with a CA file still refuses a broker that offers no TLS.
- The first connection is retried (exponential backoff, full jitter) only while
  the broker is unreachable or not yet answering the handshake; an
  authorization failure, a certificate failure and a refused client certificate
  fail at once (`NatsTlsException`, `AuthenticationException`-based
  `NatsConnectException`). `isRetryable(throwable)` is public.
- After the first connection jnats reconnects without limit, restores
  subscriptions, and this adapter keeps it trying after authorization errors
  (see the second difference below).
- `ensureStream` / `ensureConsumer`: file storage, limits retention, discard
  old, 1 replica, 7 days, 2 minute de-duplication window; explicit ack, 30s ack
  wait, 5 deliveries, 1000 unacknowledged, deliver all. They retry while
  JetStream is not answering yet (no responders, a timeout) for up to 15s.
- `publish` waits for the stream's acknowledgement (default 5s,
  `PublishOptions.timeout`), retries a missing responder three times, validates
  the subject (`validPublishSubject`) and de-duplicates by `msgId`.
- `accountForNamespace` and `validPublishSubject` are tested against the vectors
  shared with the callout and the other languages.
- `close()` drains (bounded by the drain timeout); `health()` is a round trip
  bounded to 2s.

## How the files are re-read for every (re)connect

jnats builds its `SSLContext` once, in `Options.Builder.build()`, so a rotated
CA, certificate or key would never reach a connection that way. It does take the
socket factory from that context for **each** (re)connect
(`SocketDataPort.upgradeToSecure`: `context.getSocketFactory()`, then
`createSocket(socket, host, port, true)`). So the context handed to jnats is a
thin `SSLContext` (`ReloadingSslContext`, `Tls.kt`) whose socket factory builds
a fresh, real context at every `createSocket`: it reads the CA file, the
certificate and the key anew, builds the trust and key managers, and discards
them with the socket. Nothing is cached across connections.

The token is `Options.Builder.tokenSupplier`, which jnats calls each time it
builds the `CONNECT` message, on the first connection and on every reconnect;
the supplier reads the file and trims it. An unreadable token file yields no
token (the broker then refuses it) and a log line, as in Go.

The key may be PKCS#8 (`PRIVATE KEY`), PKCS#1 RSA (`RSA PRIVATE KEY`) or SEC1 EC
(`EC PRIVATE KEY`, what cert-manager writes by default for an ECDSA key); the
JDK only parses PKCS#8, so the other two are wrapped. An encrypted key is
refused.

## Differences from Go and TypeScript

- **Blocking API, no `context`.** Calls take no context: `NatsClient.connect`
  and `retry` block, and interrupting the thread stops the first-connection
  retry; `health()` is bounded to 2s, `close()` by the drain timeout, `publish`
  by `PublishOptions.timeout`. Configuration is a data class (`NatsConfig`),
  durations are `java.time.Duration`.
- **Reconnect after authorization errors.** jnats gives up reconnecting after
  two identical authorization errors from the same server (no counterpart of
  nats.go's `IgnoreAuthErrorAbort`), and a broker that restarts can refuse
  tokens until its callout is back. jnats keys that comparison by server URI,
  so the adapter's single-server `ServerPool` hands out the broker URL with a
  fragment that changes on every reconnect attempt (the fragment is never part
  of what is dialled), and paces the attempts itself (reconnect wait plus up to
  50% jitter), because jnats paces a round only when the same server comes round
  again. `ReconnectTest` proves it against a stand-in broker that refuses
  every reconnect.
- **First-connection refusals are made prompt by closing.** After the broker
  refuses the handshake (an authorization error, a refused client certificate)
  jnats keeps waiting for the broker's PONG until the connect timeout. During
  the first connection the adapter closes the connection on the first `-ERR` or
  driver exception so the failure is immediate. After the first connection the
  driver is left alone.
- **A refused client certificate takes the connect timeout to surface.** In
  TLS 1.3 the server's refusal arrives after the client's handshake is over and
  jnats does not report it. The adapter recognises it (our certificate was sent,
  the broker never answered) and fails with `NatsTlsException`, not retried,
  but only after the connect timeout (5s by default) instead of at once. A
  verification failure on our side (unknown CA, name mismatch) is immediate.
- **Header casing.** jnats keeps a header key as given, so `injectTrace`,
  `extractTrace` and `HeaderCarrier` (over `io.nats.client.impl.Headers`) write
  lower case and read case-insensitively; `HeaderCarrier` has `get`, `set`,
  `keys`, the shape of an OpenTelemetry `TextMapGetter` / `TextMapSetter`
  without depending on OpenTelemetry.
- **Names.** `accountForNamespace` and `validPublishSubject` throw
  `IllegalArgumentException` instead of returning an error;
  `isValidPublishSubject` is the boolean form. `ensureStream` /
  `ensureConsumer` return jnats's `StreamInfo` / `ConsumerInfo`. The logger is
  the JDK's `System.Logger` (`NatsClient.connect(config, logger)`; the default
  is `com.truvity.nats`).
- **Hostnames are dialled by name** (`HostnameResolveMode.HappyEyeballs`), so
  the TLS name is the URL's host and not an address jnats resolved.

## Develop

```
cd clients/kotlin
mvn verify                    # compile and unit tests (JDK 21, Maven 3.9: devbox has both)
just clients-kotlin           # the same, plus the dry run of the release artifacts
just clients-kotlin-conformance   # from the repository root; needs docker
```

The conformance suite (`ConformanceTest`) is one test per case in
`../conformance/cases.txt`, the method name being the case name:

```
eval "$(clients/conformance/nats-broker.sh up)"
(cd clients/kotlin && NATS_CLIENTS=required mvn -B -ntp clean test -Dtest=ConformanceTest -Dsurefire.failIfNoSpecifiedTests=true)
clients/conformance/guard.sh kotlin clients/kotlin/target/surefire-reports/TEST-com.truvity.nats.ConformanceTest.xml
clients/conformance/nats-broker.sh down
```

Without `NATS_CLIENTS_URL` the suite skips; with `NATS_CLIENTS=required` it
fails instead. The guard fails unless every case ran and passed.
