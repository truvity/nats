# Development commands. Everything CI runs is a recipe here — the shared
# check workflow (truvity/ci-workflows) runs each one as its own job.

# Disable go.work (parent workspace interferes with standalone module builds)
export GOWORK := "off"

charts := "nats-auth-callout nats-broker nats-projects"

# Format all Go files (gofmt + goimports via golangci-lint)
fmt:
    golangci-lint fmt ./...

# Build the responder binary.
build: fmt
    go build -o bin/nats-auth-callout ./cmd/responder/

# Lint every chart and the Go module. The schema is part of the lint:
# an unknown key must fail the render, not be silently ignored, and every
# negative fixture under tests/invalid/<chart>/ must fail — one that
# renders is a hole in the validation nobody would otherwise notice.
# `config verify` runs first: the v2 schema silently accepts a stale
# top-level `linters-settings:` block, and only verify rejects it.
lint:
    #!/usr/bin/env bash
    set -euo pipefail
    for chart in {{ charts }}; do
      # A chart renders with WHATEVER archive is in its charts/ directory:
      # move the version in Chart.yaml and leave the vendored archive
      # behind, and Helm renders the old upstream while every check passes
      # and the golden does not move. `helm dependency list` exits 0 either
      # way, so the STATUS column is what is read.
      if [ -n "$(yq '.dependencies // [] | .[]' "charts/$chart/Chart.yaml")" ] \
         && helm dependency list "charts/$chart" \
           | tail -n +2 | grep -v '^[[:space:]]*$' | grep -qv 'ok[[:space:]]*$'; then
        helm dependency list "charts/$chart" >&2
        echo "$chart: a declared dependency is missing or is the wrong version: run 'just vendor $chart'" >&2
        exit 1
      fi
      helm lint "charts/$chart"
      # Not `! helm template ...`: bash's `set -e` ignores a command
      # negated with `!`, so such a probe could never fail the recipe.
      if helm template x "charts/$chart" --set bogusKey=1 >/dev/null 2>&1; then
        echo "$chart: an unknown key rendered" >&2
        exit 1
      fi
      for values in tests/invalid/"$chart"/*.yaml; do
        if helm template invalid "charts/$chart" -f "$values" >/dev/null 2>&1; then
          echo "RENDERED BUT SHOULD HAVE FAILED: $values" >&2
          exit 1
        fi
      done
      echo "$chart: schema and $(ls tests/invalid/"$chart"/*.yaml | wc -l | tr -d ' ') negative fixtures OK"
    done
    # The dependency pin must be the vendored archive.
    hack/check-app-version.sh
    golangci-lint config verify
    GOTOOLCHAIN=local golangci-lint run ./...

# Golden renders (every test case compared with tests/golden), the zero-diff
# gate, the alert rules' own checks, and the unit tests.
#
# The goldens show a reviewer what a change does to the render; the parity
# gate proves the nats-broker wrapper adds nothing to the upstream chart's
# own (hack/parity.sh); alerts-check proves the rules read the job the
# PodMonitor carries; rulecheck parses every rule with the real parser.
test:
    hack/golden.sh
    hack/parity.sh
    hack/alerts-check.sh
    just rulecheck
    go test ./... -coverprofile=coverage.out

# Parse every rendered rule's expressions with the real VictoriaMetrics
# parser. An expression the parser refuses is a VMRule the operator's
# admission webhook rejects and a sync that never finishes; every other
# check passes on it, because text is valid YAML whatever the expression
# inside says. truvity/observability's `rulecheck`, at a pinned version,
# on the golden renders; the parser's version is pinned beside it. An
# estate runs the same command on its own manifests:
#
#   just rulecheck path/to/rendered.yaml
#
# Downloads the release binary from github.com on first use (sha256-verified
# against the release's checksum file).
rulecheck *paths:
    #!/usr/bin/env bash
    set -euo pipefail
    paths=({{ paths }})
    if [ ${#paths[@]} -eq 0 ]; then
      paths=(tests/golden/nats-broker)
    fi
    # GOWORK off: a parent workspace must not capture a module run by version.
    GOWORK=off go run github.com/truvity/observability/cmd/rulecheck@v0.38.0 \
      -vm-version v1.152.0 -vl-version v1.52.0 "${paths[@]}"

# Re-vendor one chart's pinned upstream into its charts/ directory, after
# moving the version in its Chart.yaml. The archive is committed on purpose:
# a render that needs the network is a render that differs depending on when
# it runs. Then `just golden` and read the diff, and move appVersion to the
# upstream's.
vendor chart:
    helm dependency update charts/{{ chart }}

# Regenerate the golden renders — review the diff before committing.
golden:
    hack/golden.sh update

# The reason this repository can be public. Runs in CI as its own job.
leak-canary:
    hack/leak-canary.sh

# The TypeScript client adapter (clients/ts): lint, type check, unit tests,
# build. No broker needed; the conformance cases skip here.
clients-ts:
    #!/usr/bin/env bash
    set -euo pipefail
    cd clients/ts
    npm ci --no-audit --no-fund
    npm run lint
    npm run typecheck
    npm test
    npm run build
    # What a release would publish: the built output and the README only.
    # (The version is stamped from the tag at release time.)
    npm pack --dry-run

# The Go client adapter against a real broker with the auth callout (digest-
# pinned image, certificates generated per run; clients/conformance/). Needs
# docker. The guard fails the recipe unless every case in
# clients/conformance/cases.txt ran and passed: a skipped suite is not a green
# one. NOT part of `check` (which stays free of docker); CI runs it as its own
# job.
clients-go-conformance:
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'clients/conformance/nats-broker.sh down' EXIT
    eval "$(clients/conformance/nats-broker.sh up)"
    out="$(mktemp)"
    NATS_CLIENTS=required go test ./clients/go/... -run TestConformance -count=1 -json >"$out" || { grep -E '"Action":"(fail|output)"' "$out" | head -80 >&2; exit 1; }
    clients/conformance/guard.sh go "$out"

# The TypeScript client adapter against the same broker and case list.
clients-ts-conformance:
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'clients/conformance/nats-broker.sh down' EXIT
    eval "$(clients/conformance/nats-broker.sh up)"
    out="$(mktemp)"
    (cd clients/ts && npm ci --no-audit --no-fund && NATS_CLIENTS=required npx vitest run test/conformance.test.ts --reporter=json --outputFile="$out") || { cat "$out" >&2; exit 1; }
    clients/conformance/guard.sh ts "$out"

# The Kotlin client adapter (clients/kotlin): compile and unit tests, then the
# dry run of what a release publishes (the jar and the sources jar, no deploy).
# No broker needed; the conformance cases skip here.
clients-kotlin:
    #!/usr/bin/env bash
    set -euo pipefail
    cd clients/kotlin
    mvn -B -ntp clean verify
    mvn -B -ntp -DskipTests package
    # The version is stamped from the tag at release time.
    jar=$(ls target/nats-client-*.jar | grep -v -- '-sources' | head -1)
    jar tf "$jar" | grep -q 'com/truvity/nats/NatsClient.class'
    ls target/nats-client-*-sources.jar >/dev/null

# The Python client adapter (clients/python): lint, types, unit tests, then the
# dry run of what a release attaches to the GitHub release (wheel and sdist).
# No broker needed; the conformance cases skip here.
clients-python:
    #!/usr/bin/env bash
    set -euo pipefail
    cd clients/python
    uv sync --locked
    uv run ruff check .
    uv run ruff format --check .
    uv run mypy src tests
    uv run pytest
    rm -rf dist
    uv build
    ls dist/truvity_nats_client-*-py3-none-any.whl dist/truvity_nats_client-*.tar.gz >/dev/null

# The Kotlin client adapter against the same broker and case list.
clients-kotlin-conformance:
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'clients/conformance/nats-broker.sh down' EXIT
    eval "$(clients/conformance/nats-broker.sh up)"
    (cd clients/kotlin && NATS_CLIENTS=required mvn -B -ntp clean test -Dtest=ConformanceTest -Dsurefire.failIfNoSpecifiedTests=true)
    clients/conformance/guard.sh kotlin clients/kotlin/target/surefire-reports/TEST-com.truvity.nats.ConformanceTest.xml

# The Python client adapter against the same broker and case list.
clients-python-conformance:
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'clients/conformance/nats-broker.sh down' EXIT
    eval "$(clients/conformance/nats-broker.sh up)"
    out="$(mktemp)"
    (cd clients/python && uv sync --locked && NATS_CLIENTS=required uv run pytest tests/test_conformance.py -q --junitxml="$out") || { cat "$out" >&2; exit 1; }
    clients/conformance/guard.sh python "$out"

# Run the tests under the race detector. The responder answers callout
# requests concurrently and shares the broker connection between them, so a
# data race there would be a wrong answer rather than a crash, and would not
# show up in an ordinary run.
#
# This is not part of `check`, and deliberately. Everything else here builds
# with cgo off, which is what makes the binary static and the image small;
# the race detector is the one thing that needs a C toolchain. Putting it in
# the gate would mean every contributor needs one to run the gate at all. CI
# runs this as its own job, where the toolchain is the runner's own.
race:
    CGO_ENABLED=1 go test -race ./...

# Reachable Go advisories.
vuln:
    govulncheck ./...

# Run go mod tidy
tidy:
    go mod tidy

# Clean build artifacts
clean:
    rm -rf bin/ dist/ coverage.out

# Everything CI runs on a pull request.
check: build lint test leak-canary clients-ts clients-kotlin clients-python

# Build a snapshot release locally (no push, no tag)
snapshot:
    goreleaser release --snapshot --clean

# Package the chart locally (the release workflow stamps the version from the tag).
helm-package:
    helm package charts/nats-auth-callout --destination dist/
