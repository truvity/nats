# nats

NATS on Kubernetes, in two Helm charts: `nats-auth-callout`, the workload-identity
auth-callout responder, and `nats-broker`, a pinned wrapper over the upstream
broker chart.

## The auth-callout responder

The `nats-auth-callout` component is an auth-callout responder
that validates a connecting client's ServiceAccount token via
TokenReview and answers the broker with a signed user JWT that places
the client into the NATS account named after its namespace. No
per-client credentials, no shared passwords.

## Who it is for

A platform team that runs one NATS broker for several tenants on
Kubernetes, tenant per namespace, and wants NATS access to follow the
identity Kubernetes already issues rather than credentials it would
have to mint and distribute. It assumes a broker whose configuration
the estate renders (normally the upstream
[`nats/nats`](https://github.com/nats-io/k8s) chart), projected
ServiceAccount tokens on the clients, and a Secret the estate creates
for the one seed. It does not install the broker, does not write the
broker's `accounts` or `auth_callout` block, and does not put the seed
anywhere: it takes the name of a Secret. No cloud identity is involved;
TokenReview needs only the built-in `system:auth-delegator` grant.

## The broker chart

This repository also publishes the `nats-broker` chart: the upstream `nats` chart,
pinned and vendored, that renders **exactly what the upstream chart
renders** unless asked otherwise, with opt-in presets (restricted Pod
Security, metrics, a JetStream cluster, spreading) and opt-in alert rules.
See [docs/broker.md](docs/broker.md); moving an installation onto it is in
[docs/adoption.md](docs/adoption.md#moving-a-broker-onto-nats-broker).

## The model

Three nouns. The **broker** delegates authentication (config-mode
`auth_callout`) to an `AUTH` account whose one user is this responder.
The **responder** takes each callout request, reviews the client's
ServiceAccount token with the API server, maps the token's namespace to
the account of the same name, and signs a user JWT for that account.
The **seed**, one nkey account seed (`SA…`), is the responder's only
secret: it signs the responses, and re-encoded as a user key it is also
the responder's own login. Its two public keys, `A…` and `U…`, go into
the broker's configuration.

```
client pod (namespace my-namespace)
  │  CONNECT auth_token = projected ServiceAccount token (audience nats)
  ▼
NATS broker ── $SYS.REQ.USER.AUTH ──► responder (nats-auth-callout)
  ▲                                     │ TokenReview ──► API server
  │                                     │ system:serviceaccount:my-namespace:<name>
  └── signed user JWT, account my-namespace ◄──┘
```

Deploy the chart **beside** the broker, never instead of it. Until the
broker's `auth_callout` block is rendered the responder idles (it is
not `Ready`, because the broker refuses its login), so the order is
free to be the safe one: responder first, broker flip last.

## Install and a worked example

```sh
helm install nats-auth-callout oci://ghcr.io/truvity/charts/nats-auth-callout \
  --version <version> --namespace nats \
  --values values.yaml
```

```yaml
# values.yaml — every value is a placeholder
natsURL: nats://nats.nats.svc:4222      # the broker's client Service
issuerSecret:
  name: nats-auth-callout-issuer        # key `seed`, created by the estate
projectAccounts:
  - my-namespace                        # one NATS account per namespace
tokenAudiences:
  - nats                                # what the clients' tokens are projected with
  - https://kubernetes.default.svc      # the API server's own audience: only for controller-minted token Secrets
```

The seed Secret comes from wherever the estate keeps secrets (External
Secrets, SOPS, or `kubectl create secret generic nats-auth-callout-issuer
--from-literal seed=SA…`); `nk -gen account` mints one, and
[docs/openbao-external-secrets.md](docs/openbao-external-secrets.md)
derives its two public keys. Then the broker, last, in the upstream
chart's `config.merge`:

```yaml
config:
  merge:
    accounts:
      AUTH:
        users:
          - nkey: U…                    # the responder's login
      my-namespace: {}                  # one per projectAccounts entry
    authorization:
      auth_callout:
        issuer: A…
        auth_users: [U…]
        account: AUTH
```

A client projects its token with the audience and presents it as the
NATS token:

```yaml
volumes:
  - name: nats-token
    projected:
      sources:
        - serviceAccountToken:
            audience: nats
            expirationSeconds: 3600
            path: token
```

`tokenAudiences` is the contract: a token projected with any other
audience is denied. The API server's own audience belongs in the list
only when long-lived controller-minted token Secrets must authenticate
(a JetStream controller's, for instance): those carry the API server's
URL as their audience, which is not always `kubernetes.default.svc` (see
[docs/reference.md](docs/reference.md#audiences)). Readiness does not
need it.

`/readyz` exercises the real dependency chain (the confirmed auth
subscription, the broker connection, and a live TokenReview of the pod's
own token), so a responder that cannot authorize clients goes unready
instead of denying everything while looking `Running`. The self-review
asks for no audience, so the pod's token is accepted as the kubelet
issued it; the clients' audiences stay the clients' contract.
[docs/reference.md](docs/reference.md) has every value and every
environment variable.

## Consumers

- **truvity/gitops**: deploys the chart
- **A second, non-AWS estate**: deploys the chart

## Neighbours

- **openbao**: seed custody and External Secrets integration (see docs/openbao-external-secrets.md)
- **nats**: the broker this responder guards (upstream nats/nats chart for deployment)

## Documentation

- [docs/adoption.md](docs/adoption.md): prerequisites, the install
  order that keeps the broker flip safe, the zero-diff gate, adopting
  a responder that already runs, and upgrading
- [docs/safety.md](docs/safety.md): every render-time and start-up
  refusal and the failure it prevents; the traps, starting with the
  adoption order
- [docs/reference.md](docs/reference.md): every chart value, every
  environment variable, the mapping rule, audiences and the health
  endpoints
- [docs/doctrine.md](docs/doctrine.md): what this repository owns and
  what the consuming estate owns, and why it is shaped this way
- [docs/openbao-external-secrets.md](docs/openbao-external-secrets.md):
  the seed in OpenBAO (or Vault) delivered by External Secrets: minting,
  manifests, rotation and a troubleshooting table
- [CHANGELOG.md](CHANGELOG.md): what changed for a consumer, per version

## The rule that makes this repository public

**Mechanism only.** Nothing here names a cluster, a broker, a namespace
of the consuming estate or a secret path. Every such thing is an input
with a neutral default, and the consuming estate supplies it from its
own (private) repository. `hack/leak-canary.sh` enforces this in CI, and
public history cannot be unpublished — so the rule is mechanical, not
remembered.

This repository follows the shared
[component contract](https://github.com/truvity/policy/blob/master/docs/contracts/component.md).

## Status

Used in production by its maintainers. Releases are listed on the
[releases page](https://github.com/truvity/nats/releases),
and [CHANGELOG.md](CHANGELOG.md) says what changed for a consumer in
each.

## Development

```sh
devbox shell        # or direnv
just check          # build + lint + golden renders and go test + leak canary + govulncheck
just golden         # regenerate tests/golden after a template change — review the diff
just race           # the tests under the race detector; needs a C toolchain, so not in check
```

CI runs each recipe as its own job; `race` is its own job because
everything else builds with cgo off, and `vuln` runs daily. Every
`tests/cases/nats-auth-callout/<case>/values.yaml` is rendered and
compared byte-for-byte with `tests/golden/nats-auth-callout/<case>.yaml`;
a template change is reviewed as a diff, with no cluster involved.

`tests/invalid/nats-auth-callout/` holds one fixture per refusal. Each
must fail to render; `just check` proves it. A rule without a fixture is
a rule that will quietly stop working.

## Releasing

Push a tag `vX.Y.Z`. The shared release workflow creates the GitHub
Release with the binaries, pushes the image at that version and pushes
the chart at that version — a chart's own `version` and `appVersion`
are placeholders that never move — and the same tag is the Go module's
version.

Auto-release is **armed** (`vars.AUTO_RELEASE` is `true`), and it cuts
**patches only**: at once for a merged `security`-labelled pull request,
weekly when the default branch has moved past the latest tag. It asks
only whether the branch moved, not what moved it, so a feature merged
and left untagged ships in the next weekly patch. Minors and majors are
always manual, tagged when the change merges and after its CHANGELOG
heading.

## Licence

MIT — see [LICENSE](LICENSE).
