# The broker chart: `nats-broker`

`nats-broker` installs a NATS broker from the upstream
[`nats`](https://github.com/nats-io/k8s) chart, pinned to an exact version
and vendored into the chart, with this repository's schema, goldens and
adoption gate around it. It is the broker the
[auth-callout responder](../README.md) talks to, but neither needs the
other.

**With no values and no preset it renders exactly what the upstream chart
renders.** That is the property an adopter relies on, and `hack/parity.sh`
enforces it on every pull request.

Published as `oci://ghcr.io/truvity/charts/nats-broker`. Its version is the
repository's tag; the upstream release it wraps is the dependency in
`Chart.yaml` and is named in each [CHANGELOG](../CHANGELOG.md) entry.
The release workflow stamps `version` and `appVersion` of the published
chart with the release version, as it does for every chart in this
repository; the upstream version is the dependency.

## Values

Two namespaces, because Helm gives a subchart its own key and `global` and
nothing else.

| Key      | What goes there |
| -------- | --------------- |
| `nats`   | Every value of the upstream chart, verbatim. The upstream's `config.cluster.enabled` is `nats.config.cluster.enabled`. |
| `global` | Shared with the upstream chart as its `global`. May also sit under `nats`; the root one wins, as in Helm. |
| `alerts` | This chart's own: the alert rules, below. |

`values.schema.json` refuses any other top-level key. In particular,
upstream values pasted at the root are refused rather than ignored, which
would install the defaults.

## Presets

Presets are **values files**, shipped in the chart under `presets/`. The
chart sets none of them by default; layer the ones you want before your own
values:

```sh
helm template nats oci://ghcr.io/truvity/charts/nats-broker --version <v> \
  -f presets/restricted.yaml -f presets/metrics.yaml -f my-values.yaml
```

Argo CD: `helm.valueFiles: [presets/restricted.yaml, ...]` on an OCI source
with `path: .`, with the rest in `valuesObject`. Maps merge, lists are
replaced, and your values win.

Pull them without installing: `helm pull oci://ghcr.io/truvity/charts/nats-broker
--version <v> --untar`.

| Preset | What it sets |
| ------ | ------------ |
| `restricted.yaml` | Pod Security `restricted` on the broker, config reloader, exporter and nats-box: non-root pod user and group 1000, `fsGroup` 1000, `RuntimeDefault` seccomp, no privilege escalation, all capabilities dropped, nats-box `workingDir: /tmp`. |
| `metrics.yaml` | The prometheus-nats-exporter sidecar and its PodMonitor: what the alerts read. |
| `jetstream-cluster.yaml` | Three brokers, JetStream on a PVC file store. Size and storage class stay yours. |
| `spread.yaml` | One broker per node (hard) and across zones (soft). |
| `tenancy.yaml` | Per-tenant accounts, the auth callout and, optionally, client-certificate identities, from rows under `global.tenancy` (see below). |

Each preset is covered by a case in `tests/cases/nats-broker/presets` that is
held to the same parity gate: the upstream chart given the same values
renders the same objects.

What is deliberately not a preset: resources, node selectors, tolerations,
storage class and size. They belong to the estate.

### Tenancy: accounts, callout and client certificates

`tenancy.yaml` is the one preset with inputs. They are rows under
`global.tenancy`, because `global` is the one place a template of the
upstream chart can read the wrapper's values; the preset itself is the
upstream chart's own `$tplYamlSpread` mechanism, so the parity gate holds it
like any other values file.

```yaml
global:
  tenancy:
    authCallout: {issuer: A..., user: U...}   # the callout's issuer nkey and its user form
    accounts: [project-a, project-b, ci]      # one JetStream account per name
    identities:                               # optional: client-certificate users
      - account: project-a
        spiffeId: spiffe://trust.example/ns/project-a/sa/events
        publish: ["project-a.events.>"]
    clientCA: {configMapName: identity-ca, key: identity-ca.pem}
    serverCertificate:
      dnsName: nats.example.private
      leafKeySize: 384
      issuerRef: {name: example-private, kind: ClusterIssuer, group: cert-manager.io}
```

The `accounts` list is the same list the callout is given as
`projectAccounts` (a namespace and its account are the same name). Without
`identities` the preset renders the AUTH account, the accounts and the
`auth_callout` block and nothing else. With them the client port also speaks
TLS (`allow_non_tls` keeps the plaintext path), a TLS client must present a
certificate chaining to `clientCA`, its URI SAN is mapped to the user, and
that user may publish exactly the listed subjects and subscribe only to the
reply inbox. Every mapped user is exempt from the callout. Do not use the
preset on a broker without a callout: it renders `authorization`.

## Network policy and the NACK janitor

Two more objects the chart templates itself, both opt-in and off by default
(so the parity gate still holds, and turning them on only adds objects):

* `networkPolicy.enabled`: clients on 4222, routes on 6222, an optional
  scraper on 7777 (`networkPolicy.metricsFrom`), egress to the brokers and DNS;
  with `networkPolicy.nack.enabled` also the NACK controller's policy
  (brokers, API server, DNS). DNS and the API server are CIDR-shaped, because
  the cluster resolver and the API server's ClusterIP are not pod-backed.
* `janitor.enabled`: a CronJob that deletes NACK Stream and Consumer
  resources that have been Errored for more than `janitor.errorAfterSeconds`
  (NACK never requeues them), with its ClusterRole. `janitor.image` is required.

## Alerts

`alerts.enabled: true` renders one rule object with one group of nine rules,
written for the exporter's metrics (turn them on with `presets/metrics.yaml`).
Off by default: nothing is rendered.

| Alert | Fires when | Severity |
| ----- | ---------- | -------- |
| `NATSBrokerDown` | a broker's exporter has not answered its scrape | `severity` |
| `NATSClusterRoutesMissing` | a broker sees fewer routes than one pooled set per peer | `warningSeverity` |
| `NATSJetStreamNoMetaLeader` | JetStream's meta group has no leader | `severity` |
| `NATSStreamNoLeader` | a stream has no leader | `severity` |
| `NATSConsumerBacklogGrowing` | a consumer's backlog stayed above a floor over a window and grew | `warningSeverity` |
| `NATSSlowConsumers` | the broker cut off slow consumers within a window | `warningSeverity` |
| `NATSJetStreamStorageHigh` | JetStream's file store passed a fraction of its configured size | `warningSeverity` |
| `NATSMemoryNearLimit` | a broker's working set passed a fraction of its memory limit | `warningSeverity` |
| `NATSMetricsAbsent` | no exporter series at all (the deadman for the rest) | `warningSeverity` |

- **`alerts.kind`:** `VMRule` (VictoriaMetrics operator) or `PrometheusRule`
  (prometheus-operator). The spec is the same.
- **`alerts.namespace`, `alerts.name`, `alerts.ruleLabels`:** where the object
  lives and what a ruler's selector matches. Default: the release namespace.
- **`alerts.job`:** the exporter's scrape job. Default: derived the way the
  upstream chart names its PodMonitor (`<namespace>/<release fullname>`,
  following `nats.nameOverride`, `nats.fullnameOverride` and
  `nats.namespaceOverride`). `hack/alerts-check.sh` holds the derivation to
  the rendered PodMonitor.
- **`alerts.clusterLabel` (default `k8s_cluster_name`), `alerts.ownerLabel`,
  `alerts.commonLabels`, `alerts.keepClusterLabel`:** for a metrics store that
  holds several clusters. Every aggregation matches on the cluster label;
  `keepClusterLabel` keeps the series' own label on the alert instead of the
  common label of that name, so one ruler that evaluates several clusters
  names the cluster the broker is in. `NATSMetricsAbsent` always keeps it.
- **Thresholds:** `brokerDownFor`, `routesFor`, `leaderFor`, `backlog.*`,
  `slowConsumers.*`, `storage.*`, `memory.*`, `absentFor`, each stated in
  `values.yaml` with its meaning.

Every expression is checked by `just rulecheck`, which parses it with the real
VictoriaMetrics parser (`truvity/observability`'s `rulecheck`); a rule an
operator's admission webhook would refuse fails the pull request.

### Moving the rules

These rules were first shipped as the `nats` group of the
`truvity/observability` `platform-alerts` chart, with the same nine
expressions. To move them without a gap or a double alert: enable
`alerts` here with the same cluster-label values the group used, confirm in
the ruler that `nats` evaluates (`health: ok`), and only then turn the
observability group off. A ruler refuses two groups of one name in one file,
so this chart's group is named `nats`, not the old `platform-alerts.nats`,
which is what lets both exist for the length of the overlap. The `alertgroup`
label on the alert changes with the name; check that no route matches it.
