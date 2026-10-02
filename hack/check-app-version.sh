#!/usr/bin/env bash
# nats-broker's appVersion must equal the appVersion of the upstream archive
# it vendors. The release workflow stamps appVersion on a published chart,
# so this is what keeps the checked-in value honest when renovate moves the
# dependency: move the version in Chart.yaml and re-vendor
# (`just vendor`), then move appVersion to the upstream's.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
chart_dir="$root/charts/nats-broker"
want="$(yq '.appVersion' "$chart_dir/Chart.yaml")"
dep="$(yq '.dependencies[0].version' "$chart_dir/Chart.yaml")"
archive="$(ls "$chart_dir"/charts/*.tgz)"
have="$(tar -xzOf "$archive" nats/Chart.yaml | yq '.appVersion')"
chart_have="$(tar -xzOf "$archive" nats/Chart.yaml | yq '.version')"
fail=0
if [ "$want" != "$have" ]; then
  echo "nats-broker: appVersion is $want but the vendored upstream says $have" >&2
  fail=1
fi
if [ "$dep" != "$chart_have" ]; then
  echo "nats-broker: the dependency is pinned at $dep but the vendored archive is $chart_have" >&2
  fail=1
fi
[ "$fail" = 0 ] && echo "appVersion $want matches the vendored upstream chart $chart_have"
exit $fail
