#!/usr/bin/env bash
# The rules name the exporter's scrape job (`<namespace>/<podmonitor>`) and
# the broker's namespace, and derive both from the release because the
# upstream chart names its PodMonitor from the release. A derivation that
# drifts from the upstream's naming is a set of rules that parse, pass every
# check and never fire. So for every case that turns the alerts on, the job
# in the rendered rules must equal the namespace and name of the rendered
# PodMonitor, unless `alerts.job` says otherwise.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
chart=nats-broker
fail=0
n=0

for values in "$root"/tests/cases/"$chart"/*/values.yaml; do
  case_dir="$(dirname "$values")"
  case_name="$(basename "$case_dir")"
  [ "$(yq '.alerts.enabled // false' "$values")" = true ] || continue
  [ -z "$(yq '.alerts.job // ""' "$values")" ] || continue
  ns="$(cat "$case_dir/namespace" 2>/dev/null || echo default)"
  release="$(cat "$case_dir/release" 2>/dev/null || echo "$chart")"
  args=()
  if [ -f "$case_dir/presets" ]; then
    while read -r preset; do
      [ -n "$preset" ] && args+=(-f "$root/charts/$chart/presets/$preset.yaml")
    done < "$case_dir/presets"
  fi
  out="$(helm template "$release" "$root/charts/$chart" --namespace "$ns" "${args[@]}" -f "$values")"
  pm_name="$(printf '%s\n' "$out" | yq eval-all 'select(.kind == "PodMonitor") | .metadata.name' -)"
  pm_ns="$(printf '%s\n' "$out" | yq eval-all 'select(.kind == "PodMonitor") | .metadata.namespace' -)"
  jobs="$(printf '%s\n' "$out" | grep -o 'job="[^"]*"' | sort -u)"
  # The upstream chart omits the PodMonitor's namespace when it is the
  # release's own; the scrape job then carries the release namespace.
  [ "$pm_ns" = null ] && pm_ns="$ns"
  want="job=\"$pm_ns/$pm_name\""
  n=$((n + 1))
  if [ "$jobs" != "$want" ]; then
    echo "ALERTS JOB DRIFT: $chart/$case_name — the rules read ${jobs:-no job} but the PodMonitor is $want" >&2
    fail=1
  fi
done

[ "$n" -gt 0 ] || { echo "alerts-check: no case turns the alerts on" >&2; exit 1; }
[ "$fail" = 0 ] && echo "alerts: $n cases read the scrape job the PodMonitor carries"
exit $fail
