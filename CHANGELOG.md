# Changelog

What changed for a consumer, per version, newest first. A version with no
heading here is a patch cut automatically for dependency bumps alone; its
GitHub Release lists them. The chart and the image are released together
at every version.

## v1.6.0

- **`pkg/tenancy` (new Go package) and a values schema for `global.tenancy.identities`.** `tenancy.Identities` maps a project's account, the environment's trust domain, its ServiceAccounts and the subjects they may publish to the preset's `identities` rows (`transport.SpiffeID` builds the ID), and `tenancy.ValidateClient` holds a deployment's catalogue to the rules the identities exist under. The chart's values schema now checks each `global.tenancy.identities` row: `spiffeId` is `spiffe://<trust domain>/ns/<namespace>/sa/<ServiceAccount>`, and `publish` is a non-empty, unique list of **concrete** subjects. **A wildcard in `publish` is now refused** (the preset's own comment always said the exact subjects): an identity exists to do one job, and a wildcard hands it the stream's whole subject space. The preset's templates and every render of a valid input are unchanged.

- **Internal:** the zero-diff gate (`hack/parity.sh`) is now a Go test, `tests/proof`, built on the shared `parity.Wrapper` of `github.com/truvity/cd/parity` instead of a private shell copy. Same cases, same verdict; no chart changes, no render changes.

## v1.5.0

- **`nats-projects`, a new chart.** The NATS account of every project
  namespace from a list of project rows, for an L3 `-projects` Application:
  per row the ServiceAccount NACK presents, its token Secret and the NACK
  `Account` named after the namespace (the callout's 1:1 rule). Accounts
  carry `Prune=false,Delete=false`. The broker's `global.tenancy.accounts`
  and the callout's `projectAccounts` are the same list of names.

## v1.4.0

- **`nats-broker`: the `tenancy` preset.** Per-tenant accounts, the auth
  callout and, optionally, client-certificate identities (TLS beside the
  plaintext path, a mapped user per SPIFFE ID, the broker's server
  Certificate), from rows under `global.tenancy`. Values only: it uses the
  upstream chart's `$tplYamlSpread`, so the parity gate holds it
  (`docs/broker.md`, "Tenancy").
- **`nats-broker`: opt-in `networkPolicy` and `janitor`.** The brokers' and
  NACK's NetworkPolicies, and the NACK Errored-CR janitor, moved here from an
  estate. Off by default: with neither set the render is unchanged.

## v1.3.2

- Dependency updates.

## v1.3.1

- **`nats-broker` alerts: `NATSMetricsAbsent` ignores series without the
  cluster label.** With `alerts.clusterLabel` set, the per-cluster
  comparison of the series seen within `absentLookback` against the ones
  present now took in a stale series lacking the cluster label (left over
  from before a relabel); it sat in the lookback side under a label set the
  current side could never match, so the alert fired on a healthy broker
  until it aged out. The comparison now requires `<clusterLabel>!=""` on
  both sides; the whole-store `absent()` line is unchanged. Alerts for a
  series that was labelled and stopped still fire. The same change is in
  `truvity/observability`'s `platform-alerts` chart, so the two copies stay
  equal.

## v1.3.0

- **New chart: `nats-broker`,** published as
  `oci://ghcr.io/truvity/charts/nats-broker`. It wraps the upstream `nats`
  chart 2.15.0 (NATS server 2.15.0), values nested one level under `nats`.
  With no values and no preset the render is the upstream chart's, object for
  object (`hack/parity.sh`, every case in `tests/cases/nats-broker`). A
  chart's version is the repository's tag, so this is the first release to
  carry it and its version starts at 1.3.0, not 0.x; the nats-auth-callout
  chart and the responder image are unchanged.
- **Opt-in presets** (values files under `presets/`, none applied by
  default): `restricted` (Pod Security restricted on the broker, reloader,
  exporter and nats-box), `metrics` (exporter and PodMonitor),
  `jetstream-cluster` and `spread`.
- **Opt-in `alerts`** (off by default): nine rules for the broker's metrics
  as a VMRule or a PrometheusRule (`alerts.kind`), the same expressions the
  `nats` group of `truvity/observability`'s `platform-alerts` chart carries,
  with `keepClusterLabel` for a store that holds several clusters. See
  docs/broker.md, "Moving the rules".
- `values.schema.json` refuses an unknown top-level key, in particular
  upstream values pasted at the root.

## v1.2.1

- **Fix:** releases publish to the renamed repository. v1.2.0 was tagged but never published — its release run failed — so v1.2.1 is the first release with the new module path `github.com/truvity/nats` and image `ghcr.io/truvity/nats/responder`; the v1.2.0 notes below apply to it.

## v1.2.0

- **Breaking: the Go module path is now `github.com/truvity/nats`.** The
  repository was renamed from `nats-auth-callout` to `nats`; the module
  path follows it, inside v1. Upgrade step: replace the import path
  `github.com/truvity/nats-auth-callout` with `github.com/truvity/nats`
  in `go.mod` and every import. The package directory and the Go package
  name (`natsauthcallout`) are unchanged.
- **Breaking: the responder image is now
  `ghcr.io/truvity/nats/responder`** (was
  `ghcr.io/truvity/nats-auth-callout/responder`). The chart default
  `image.repository` points at the new path, so a chart consumer only
  bumps the chart version. Upgrade step: an install that pins the image
  directly, or overrides `image.repository`, replaces the old path with
  the new one. Older tags stay at the old path.

## v1.1.0

- `tolerations` now defaults to `[]`; the estate-shaped `arch` toleration is no longer implied. Set it in values where the estate needs it.
- README gains `Consumers` and `Neighbours`; ci-workflows pins moved to v3.13.1.

## v1.0.8

- **`/readyz` reviews the pod's own token without an audience list.** The
  kubelet projects that token with the API server's audience alone, so
  the self-review no longer needs `tokenAudiences` to name the API
  server: the default `[nats]` gives a `Ready` responder. Clients are
  still reviewed against `tokenAudiences` only. An install that listed
  the API server's audience for readiness may drop it; keep it where
  long-lived controller-minted token Secrets must authenticate. The
  library constructor `NewHealthServer` loses its audiences parameter.

## v1.0.1

- **`values.schema.json` admits the chart's own `natsURL: ""`
  placeholder.** v1.0.0's rule accepted only a non-empty `nats://` URL,
  so `helm lint` on a clean checkout failed on the shipped default. Empty
  now passes the schema and is still refused by the template's
  `required`, so an install without a real `natsURL` fails exactly as
  before; anything non-empty must still be a `nats://` URL. A values file
  that installed before installs unchanged.

## v1.0.0

- First release: the `nats-auth-callout` chart and the responder image,
  with the strict `values.schema.json`, the egress NetworkPolicy (off by
  default), TokenReview retry and the decision cache, and `/readyz`
  exercising the real dependency chain.
