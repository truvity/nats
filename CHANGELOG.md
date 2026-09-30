# Changelog

What changed for a consumer, per version, newest first. A version with no
heading here is a patch cut automatically for dependency bumps alone; its
GitHub Release lists them. The chart and the image are released together
at every version.

## Unreleased

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
