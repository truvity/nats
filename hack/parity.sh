#!/usr/bin/env bash
# The zero-diff gate: the nats-broker wrapper renders the same objects as the
# upstream nats chart for every case under tests/cases/nats-broker (see
# tests/proof for the claim). The proof is the shared `parity.Wrapper` of
# github.com/truvity/cd/parity, run as a Go test; this script is its entry point
# for `just test` and CI. An estate proves its own adoption the same way:
# docs/adoption.md.
set -euo pipefail

cd "$(dirname "$0")/.."
exec go test -count=1 ./tests/proof/
