#!/usr/bin/env bash
# nats-broker's dependency pin must equal the version of the upstream archive
# it vendors. A chart renders with WHATEVER archive is in its charts/
# directory: move the version in Chart.yaml and leave the archive behind,
# and Helm renders the old upstream while every check passes. Re-vendor with
# `just vendor nats-broker`, then `just golden` and read the diff.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
chart_dir="$root/charts/nats-broker"
dep="$(yq '.dependencies[0].version' "$chart_dir/Chart.yaml")"
archive="$(ls "$chart_dir"/charts/*.tgz)"
have="$(tar -xzOf "$archive" nats/Chart.yaml | yq '.version')"
app="$(tar -xzOf "$archive" nats/Chart.yaml | yq '.appVersion')"
if [ "$dep" != "$have" ] || [ "$dep" != "$app" ]; then
  echo "nats-broker: the dependency is pinned at $dep but the vendored archive is chart $have (server $app)" >&2
  exit 1
fi
echo "dependency $dep matches the vendored upstream chart and its server version"
