#!/usr/bin/env bash
# The zero-diff gate. For every case under tests/cases/nats-broker/<case>/:
#
#   1. render the WRAPPER chart with the case's presets and values;
#   2. flatten the same values to the UPSTREAM chart's own shape (the `nats`
#      key's contents, with the root `global` merged over the nested one,
#      which is Helm's precedence), and render the vendored upstream archive
#      alone, under the same release name and namespace;
#   3. require the two renders to be the same objects.
#
# That is the claim an adopter relies on: moving an installation from the
# upstream chart to this one, with its values nested one level, changes
# nothing the cluster sees. It holds because everything this chart adds is
# either off until asked for or is a values file:
#
#   - the presets (charts/nats-broker/presets/*.yaml) are values, so a case
#     that names them is compared against the upstream chart given the same
#     values, flattened;
#   - the alert rules are the one object the chart templates itself, and
#     they appear only when `alerts.enabled` is true. A case that turns them
#     on is held to two claims: the render with `alerts.enabled=false`
#     equals the upstream's, and turning them on ADDS one rule object and
#     changes nothing else.
#
# The day a default or a template changes what the wrapper renders for
# values that do not ask for it, this script fails, and the change is a
# Behaviour change declared in CHANGELOG.md rather than a surprise.
#
# "The same objects" is the render with its `# Source:` comment lines
# removed. Those name the template's path (`nats/templates/...` against
# `nats-broker/charts/nats/templates/...`) and Helm emits them as comments,
# so no cluster, no ArgoCD comparison and no `kubectl diff` ever sees them.
# Everything else is compared byte for byte.
#
# An estate proves its own adoption the same way, with ITS values, before
# it merges the change that moves the source: docs/adoption.md.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
chart=nats-broker
key=nats
fail=0
n=0
alerts_n=0
own_n=0
upstream="$(ls "$root"/charts/"$chart"/charts/"$key"-*.tgz)"

strip() { grep -v '^# Source:' || true; }

for values in "$root"/tests/cases/"$chart"/*/values.yaml; do
  case_dir="$(dirname "$values")"
  case_name="$(basename "$case_dir")"
  ns="$(cat "$case_dir/namespace" 2>/dev/null || echo default)"
  release="$(cat "$case_dir/release" 2>/dev/null || echo "$chart")"
  tmp="$(mktemp -d)"

  files=()
  if [ -f "$case_dir/presets" ]; then
    while read -r preset; do
      [ -n "$preset" ] && files+=("$root/charts/$chart/presets/$preset.yaml")
    done < "$case_dir/presets"
  fi
  files+=("$values")

  args=()
  for f in "${files[@]}"; do args+=(-f "$f"); done

  # The files layered in order, deep-merged, maps merged and lists replaced:
  # what Helm hands the chart.
  yq eval-all '. as $item ireduce ({}; . * $item)' "${files[@]}" > "$tmp/merged.yaml"
  # shellcheck disable=SC2016
  yq eval "(.[\"$key\"] // {}) * {\"global\": ((.[\"$key\"].global // {}) * (.global // {}))}" \
    "$tmp/merged.yaml" > "$tmp/flat.yaml"

  helm template "$release" "$upstream" --namespace "$ns" -f "$tmp/flat.yaml" | strip > "$tmp/upstream.yaml"

  alerts_on="$(yq '.alerts.enabled // false' "$tmp/merged.yaml")"
  own_on="$(yq '(.alerts.enabled // false) or (.networkPolicy.enabled // false) or (.janitor.enabled // false)' "$tmp/merged.yaml")"
  n=$((n + 1))

  if [ "$own_on" = true ]; then
    own_n=$((own_n + 1))
    helm template "$release" "$root/charts/$chart" --namespace "$ns" "${args[@]}" \
      --set alerts.enabled=false --set networkPolicy.enabled=false --set janitor.enabled=false | strip > "$tmp/wrapped.yaml"
    helm template "$release" "$root/charts/$chart" --namespace "$ns" "${args[@]}" | strip > "$tmp/with-alerts.yaml"
    # Turning the chart's own objects on only ADDS: no line of the other objects moves.
    if diff "$tmp/wrapped.yaml" "$tmp/with-alerts.yaml" | grep -q '^<'; then
      echo "PARITY BROKEN: $chart/$case_name — turning the chart's own objects on changed an object other than theirs:" >&2
      diff -u "$tmp/wrapped.yaml" "$tmp/with-alerts.yaml" | grep -E '^-[^-]' | head -20 >&2
      fail=1
    fi
  fi
  if [ "$alerts_on" = true ]; then
    alerts_n=$((alerts_n + 1))
    # And what the alerts add is one object, of the kind alerts.kind names.
    kind="$(yq '.alerts.kind // "VMRule"' "$tmp/merged.yaml")"
    before="$(grep -c "^kind: $kind\$" "$tmp/wrapped.yaml" || true)"
    after="$(grep -c "^kind: $kind\$" "$tmp/with-alerts.yaml" || true)"
    total_before="$(grep -c '^kind: ' "$tmp/wrapped.yaml" || true)"
    total_after="$(grep -c '^kind: ' "$tmp/with-alerts.yaml" || true)"
    # (A case that also turns on the policies or the janitor adds more objects,
    # so the count is only held where the rules are the only extra.)
    others_on="$(yq '(.networkPolicy.enabled // false) or (.janitor.enabled // false)' "$tmp/merged.yaml")"
    if [ "$others_on" = false ] && { [ $((after - before)) != 1 ] || [ $((total_after - total_before)) != 1 ]; }; then
      echo "PARITY BROKEN: $chart/$case_name — alerts.enabled=true must add exactly one $kind and nothing else" >&2
      fail=1
    fi
  fi
  if [ "$own_on" != true ]; then
    helm template "$release" "$root/charts/$chart" --namespace "$ns" "${args[@]}" | strip > "$tmp/wrapped.yaml"
  fi

  if ! diff -u "$tmp/upstream.yaml" "$tmp/wrapped.yaml" > "$tmp/diff"; then
    echo "PARITY BROKEN: $chart/$case_name — the wrapper renders something the upstream chart does not:" >&2
    head -40 "$tmp/diff" >&2
    fail=1
  fi
  rm -rf "$tmp"
done

[ "$fail" = 0 ] && echo "parity: $n cases render the same objects through the wrapper as through the upstream chart ($own_n of them with the chart's own objects on: only added, nothing else moved)"
exit $fail
