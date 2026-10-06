# natsclient (Go)

`github.com/truvity/nats/clients/go/natsclient`, the Go adapter of the
[contract](../README.md). It wraps nats.go (`github.com/nats-io/nats.go`).

```go
cfg, err := natsclient.FromEnv(os.Getenv)   // NATS_URL, NATS_CA_FILE, NATS_CERT_FILE, ...
if err != nil { /* every problem at once */ }
c, err := natsclient.Connect(ctx, cfg, natsclient.WithLogger(logger))
defer c.Close(ctx)                           // drains

_, err = c.EnsureStream(ctx, natsclient.StreamSpec{Name: "ORDERS", Subjects: []string{"orders.>"}, Replicas: 3})
_, err = c.Publish(ctx, "orders.created", payload, natsclient.PublishOptions{MsgID: id, Traceparent: tp})

sub, _ := c.Conn().Subscribe("orders.created", handler)   // the driver's connection, for the rest
if err := c.Health(ctx); err != nil { /* not ready */ }
```

- **Reloading credentials.** The CA is read in the TLS handshake (`VerifyConnection`
  against the file's current content; the standard library cannot take a CA pool
  that changes, so the built-in verification is replaced by one that does not
  skip any step), the certificate by `GetClientCertificate`, the token by
  nats.go's `TokenHandler`, each on every (re)connect.
- **Tracing.** `natsclient.HeaderCarrier(msg.Header)` satisfies OpenTelemetry's
  `TextMapCarrier` (`Get`, `Set`, `Keys`) with the contract's header casing:
  `otel.GetTextMapPropagator().Inject(ctx, natsclient.HeaderCarrier(msg.Header))`.
- **What it does not wrap.** Subscriptions, requests, key-value and object
  stores are the driver's; `Client.Conn()` and `Client.JetStream()` give them.
  Do not close the connection yourself; use `Close`.
- **Mapping rule.** The adapter has its own copy of `AccountForNamespace`, not an
  import of the callout's, because the callout's package pulls Kubernetes client
  libraries into a consumer's binary. The shared vectors test both.
