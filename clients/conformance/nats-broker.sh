#!/usr/bin/env bash
# A broker for the client conformance suites: the nats server (image pinned by
# digest) with the estate's tenancy shape, and the real auth-callout responder
# beside it.
#
#   nats-broker.sh up    start both; print `export` lines on stdout (all else on stderr)
#   nats-broker.sh down  remove them (and the certificate directory)
#
# Shape (charts/nats-broker/presets/tenancy.yaml): accounts AUTH, shop and other
# (JetStream on); a callout in AUTH; the client port speaks TLS beside the
# plaintext path (allow_non_tls); a TLS client must present a certificate that
# chains to the CA, and its URI SAN is mapped to a user (verify_and_map) that
# bypasses the callout and may only publish `conformance.>`. A plaintext client
# presents a token the callout validates. The callout is the real responder with
# the Kubernetes TokenReview replaced by a file of known tokens (callout/).
#
# Everything is generated here, per run: a CA, a second unrelated CA, a server
# certificate that carries ONLY the name `localhost`, two client certificates
# for two identities (rotation), and one from the wrong authority. Nothing is a
# secret and nothing outlives the run.
set -euo pipefail

IMAGE="nats:2.15.0@sha256:cd3fcd4ecdda44e3a66728a5334af0a959bc3979b32810e033d1c547241cd0f4"
NAME="${NATS_CLIENTS_NAME:-nats-clients-$$}"
STATE="${NATS_CLIENTS_STATE:-${TMPDIR:-/tmp}/nats-clients.state}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
TRUST="conformance.test"

log() { echo "nats-broker: $*" >&2; }

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

up() {
  local dir port mon
  dir="$(mktemp -d)"
  chmod 755 "$dir"
  cd "$dir"

  # --- authorities ---------------------------------------------------------
  for ca in ca other-ca; do
    openssl ecparam -name prime256v1 -genkey -noout -out "$ca.key" 2>/dev/null
    openssl req -x509 -new -key "$ca.key" -sha256 -days 2 -subj "/CN=nats-clients-$ca" -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -out "$ca.crt" 2>/dev/null
  done

  # --- server: the name `localhost` and nothing else -------------------------
  openssl ecparam -name prime256v1 -genkey -noout -out server.key 2>/dev/null
  openssl req -new -key server.key -subj "/CN=localhost" -out server.csr 2>/dev/null
  printf 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n' >server.ext
  openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 2 -sha256 -extfile server.ext -out server.crt 2>/dev/null

  # --- clients: a SPIFFE URI SAN each; api and worker (rotation) ----------------
  mkclient() { # mkclient <name> <ca> <sa>
    openssl ecparam -name prime256v1 -genkey -noout -out "$1.key" 2>/dev/null
    openssl req -new -key "$1.key" -subj "/CN=$3" -out "$1.csr" 2>/dev/null
    printf 'subjectAltName=URI:spiffe://%s/ns/shop/sa/%s\nextendedKeyUsage=clientAuth\n' "$TRUST" "$3" >"$1.ext"
    openssl x509 -req -in "$1.csr" -CA "$2.crt" -CAkey "$2.key" -CAcreateserial -days 2 -sha256 -extfile "$1.ext" -out "$1.crt" 2>/dev/null
  }
  mkclient api ca api
  mkclient worker ca worker
  mkclient foreign other-ca api   # the right identity from the WRONG authority
  chmod 644 ./*
  chmod 600 ./*.key

  # --- issuer key, tokens ---------------------------------------------------
  # shellcheck disable=SC1090
  eval "$(cd "$ROOT" && GOWORK=off go run ./clients/conformance/callout keygen)"
  cat >tokens.json <<JSON
{"token-shop-api": "shop/api", "token-shop-worker": "shop/worker", "token-other-app": "other/app", "token-kube-system": "kube-system/default"}
JSON
  (cd "$ROOT" && GOWORK=off go build -o "$dir/callout" ./clients/conformance/callout)

  local api_id="spiffe://$TRUST/ns/shop/sa/api" worker_id="spiffe://$TRUST/ns/shop/sa/worker"
  cat >nats.conf <<CONF
port: 4222
http: 8222
server_name: conformance
jetstream { store_dir: "/data", max_memory_store: 64MB, max_file_store: 256MB }
allow_non_tls: true
tls {
  cert_file: "/nats/server.crt"
  key_file: "/nats/server.key"
  ca_file: "/nats/ca.crt"
  verify_and_map: true
  min_version: "1.3"
}
accounts {
  AUTH { users: [ { nkey: "$ISSUER_USER_PUB" } ] }
  shop {
    jetstream: enabled
    users: [
      { user: "$api_id",    permissions: { publish: { allow: ["conformance.>"] }, subscribe: { allow: ["_INBOX.>"] } } }
      { user: "$worker_id", permissions: { publish: { allow: ["conformance.>"] }, subscribe: { allow: ["_INBOX.>"] } } }
    ]
  }
  other { jetstream: enabled }
}
authorization {
  auth_callout {
    issuer: "$ISSUER_PUB"
    auth_users: ["$ISSUER_USER_PUB", "$api_id", "$worker_id"]
    account: AUTH
  }
}
CONF

  port="$(free_port)"
  mon="$(free_port)"
  docker run -d --name "$NAME" -p "127.0.0.1:$port:4222" -p "127.0.0.1:$mon:8222" \
    -v "$dir:/nats:ro" "$IMAGE" -c /nats/nats.conf >/dev/null

  local i
  for i in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:$mon/healthz" >/dev/null 2>&1 && break
    sleep 0.5
    if [ "$i" = 60 ]; then log "the broker did not become ready"; docker logs "$NAME" >&2 || true; exit 1; fi
  done

  NATS_URL="nats://127.0.0.1:$port" NATS_ISSUER_SEED="$ISSUER_SEED" NATS_PROJECT_ACCOUNTS="shop,other" \
    CALLOUT_TOKENS_FILE="$dir/tokens.json" nohup "$dir/callout" serve >"$dir/callout.log" 2>&1 &
  local cpid=$!
  echo "$NAME $dir $cpid" >"$STATE"
  # Ready when a token client is let in.
  for i in $(seq 1 60); do
    if ! kill -0 "$cpid" 2>/dev/null; then log "the callout exited"; cat "$dir/callout.log" >&2; exit 1; fi
    if grep -q "listening for authorization requests" "$dir/callout.log" 2>/dev/null; then break; fi
    sleep 0.5
    if [ "$i" = 60 ]; then log "the callout did not become ready"; cat "$dir/callout.log" >&2; exit 1; fi
  done
  log "up: $NAME on 127.0.0.1:$port (monitor $mon), certificates in $dir"

  cat <<ENV
export NATS_CLIENTS_URL=nats://127.0.0.1:$port
export NATS_CLIENTS_TLS_URL=tls://localhost:$port
export NATS_CLIENTS_TLS_URL_IP=tls://127.0.0.1:$port
export NATS_CLIENTS_MONITOR=http://127.0.0.1:$mon
export NATS_CLIENTS_DIR=$dir
export NATS_CLIENTS_CONTAINER=$NAME
export NATS_CLIENTS_TRUST_DOMAIN=$TRUST
ENV
}

down() {
  [ -f "$STATE" ] || exit 0
  local name dir cpid
  read -r name dir cpid <"$STATE"
  [ -n "$cpid" ] && kill "$cpid" 2>/dev/null || true
  docker rm -f "$name" >/dev/null 2>&1 || true
  [ -n "$dir" ] && rm -rf "$dir"
  rm -f "$STATE"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
