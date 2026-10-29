#!/usr/bin/env bash
# Supplemental focused Matching dependency oracles (runs on a GitHub-hosted
# runner only; never run Cargo/Go locally on the dev host):
#   1. cadence-matching  matching_history_tchannel::tests::
#      generated_go_history_server_accepts_native_tchannel_calls  (--ignored)
#      against the original Go peer go/tools/cadence-rust-history-peer.
#   2. cadence-persistence matching_postgres::tests::postgres_tls_live_oracle
#      (--ignored) against a disposable TLS-enabled PostgreSQL container.
#   3. cadence-persistence matching_postgres::native::tests::
#      live_native_store_raw_bytes_fencing_and_legacy_cross_read (--ignored)
#      against that disposable PostgreSQL on the original Go schema.
#   4. cadence-persistence matching_postgres::tests::
#      postgres_pool_return_wakes_waiters_live_oracle (--ignored).
# Not a substitute for the full mixed-cluster lanes.
#
# Required env: EVIDENCE_DIR (retained artifact dir), SECRET_DIR (never
# uploaded; holds the ephemeral CA/server key/password), CARGO_TARGET_DIR.
set -u
: "${EVIDENCE_DIR:?}" "${SECRET_DIR:?}" "${CARGO_TARGET_DIR:?}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PG_IMAGE="${PG_IMAGE:-postgres:16}"
CONTAINER="cadence-dep-oracle-pg-$$"
GO_TEST=matching_history_tchannel::typed_codec_tests::generated_go_history_server_accepts_native_tchannel_calls
PG_TEST=matching_postgres::tests::postgres_tls_live_oracle
PG_NATIVE_TEST=matching_postgres::native::tests::live_native_store_raw_bytes_fencing_and_legacy_cross_read
PG_POOL_TEST=matching_postgres::tests::postgres_pool_return_wakes_waiters_live_oracle
OVERALL=0

mkdir -p "$EVIDENCE_DIR" "$SECRET_DIR"
chmod 700 "$SECRET_DIR"
log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$EVIDENCE_DIR/gate.log"; }
fail() { OVERALL=1; log "FAIL: $*"; }
# Record an exit code in <name>.exit and fold it into the overall result.
record() { printf '%s\n' "$2" > "$EVIDENCE_DIR/$1.exit"; [ "$2" -eq 0 ] || fail "$1 exit=$2"; }

PG_PASSWORD="$(openssl rand -hex 24)"
export CADENCE_POSTGRES_PASSWORD="$PG_PASSWORD" POSTGRES_PASSWORD="$PG_PASSWORD"

cleanup() {
  local stage_rc=$? rc=0 remaining
  if docker inspect "$CONTAINER" >/dev/null 2>&1; then
    docker logs "$CONTAINER" 2>&1 | sed "s/$PG_PASSWORD/<redacted>/g" | tail -n 200 > "$EVIDENCE_DIR/postgres-container.log"
    docker rm -f "$CONTAINER" > "$EVIDENCE_DIR/cleanup.log" 2>&1 || rc=1
  else
    echo "container_not_created_or_already_removed=true" > "$EVIDENCE_DIR/cleanup.log"
  fi
  remaining="$(docker ps -aq --filter "name=^/${CONTAINER}$")" || rc=1
  if [ -n "$remaining" ]; then
    echo "containers_left=true" >> "$EVIDENCE_DIR/cleanup.log"
    rc=1
  else
    echo "containers_left=false" >> "$EVIDENCE_DIR/cleanup.log"
  fi
  sudo -n rm -rf "$SECRET_DIR" 2>/dev/null || rm -rf "$SECRET_DIR"
  if [ -e "$SECRET_DIR" ]; then
    echo "secret_dir_removed=false" >> "$EVIDENCE_DIR/cleanup.log"
    rc=1
  else
    echo "secret_dir_removed=true" >> "$EVIDENCE_DIR/cleanup.log"
  fi
  printf '%s\n' "$rc" > "$EVIDENCE_DIR/cleanup.exit"
  if [ "$rc" -ne 0 ] || [ "$stage_rc" -ne 0 ]; then OVERALL=1; fi
  printf '%s\n' "$OVERALL" > "$EVIDENCE_DIR/overall.exit"
  trap - EXIT
  exit "$OVERALL"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

SRC_SHA="$(git -C "$ROOT" rev-parse HEAD)"
printf '%s\n' "$SRC_SHA" > "$EVIDENCE_DIR/source-sha.txt"
log "source SHA $SRC_SHA"
{ rustc --version; cargo --version; go version; docker --version; echo "image=$PG_IMAGE"; } > "$EVIDENCE_DIR/toolchains.txt" 2>&1

# Prints the path of the lib test executable Cargo itself reported for a package.
find_test_exe() { # <json> <package-dir-name>
  python3 -I - "$1" "$2" <<'PY'
import json, sys
path, crate = sys.argv[1], sys.argv[2]
found = []
for line in open(path):
    try:
        m = json.loads(line)
    except ValueError:
        continue
    if m.get("reason") != "compiler-artifact" or not m.get("executable"):
        continue
    if not m.get("profile", {}).get("test"):
        continue
    if "lib" not in m["target"]["kind"]:
        continue
    if m["target"]["src_path"].find("/crates/%s/src/" % crate) < 0:
        continue
    found.append(m["executable"])
if len(found) != 1:
    sys.exit("expected exactly one lib test executable for %s, got %r" % (crate, found))
print(found[0])
PY
}

# Build the test executable and run one --ignored test exactly.
# build_and_run <label> <package> <test path> [extra env assignments via ENVV array]
build_test_exe() { # <label> <package>
  local label="$1" pkg="$2"
  (cd "$ROOT/rust" && cargo test --locked -p "$pkg" --lib --no-run --message-format=json \
    > "$EVIDENCE_DIR/$label-build.json" 2> "$EVIDENCE_DIR/$label-build.log")
  local rc=$?
  record "$label-build" "$rc"
  [ "$rc" -eq 0 ] || return 1
  EXE="$(find_test_exe "$EVIDENCE_DIR/$label-build.json" "$pkg" 2> "$EVIDENCE_DIR/$label-exe.err")" || { fail "$label: cannot locate test executable"; return 1; }
  sha256sum "$EXE" | sed "s#$EXE#$pkg-lib-tests#" > "$EVIDENCE_DIR/$label-binary.sha256"
  printf '%s\n' "$EXE" > "$EVIDENCE_DIR/$label-binary.path"
  # Large JSON is only needed to locate the executable.
  gzip -f "$EVIDENCE_DIR/$label-build.json"
  log "$label test executable $(cat "$EVIDENCE_DIR/$label-binary.sha256")"
}

# Cargo runs lib tests from the package directory; do the same.
run_exact_ignored() { # <label> <pkg> <test> <logname>
  (cd "$ROOT/rust/crates/$2" &&
    if [ -n "${ORACLE_TIMEOUT:-}" ]; then
      timeout --kill-after=5s "$ORACLE_TIMEOUT" "$EXE" --ignored --exact "$3" --nocapture --test-threads=1
    else
      "$EXE" --ignored --exact "$3" --nocapture --test-threads=1
    fi) \
    2>&1 | sed "s/$PG_PASSWORD/<redacted>/g" > "$EVIDENCE_DIR/$4.log"
  local rc=${PIPESTATUS[0]}
  record "$4" "$rc"
  # The ignored test must really have executed (not 0 tests / filtered).
  if ! grep -q 'test result: ok. 1 passed; 0 failed; 0 ignored' "$EVIDENCE_DIR/$4.log"; then
    fail "$4: expected exactly one executed passing test"
    return 1
  fi
  return "$rc"
}

########################################################################
# Oracle 1: original Go History peer <-> Rust native TChannel client
########################################################################
log "== Oracle 1: Go History peer wire oracle"
PEER_BIN="$(dirname "$SECRET_DIR")/cadence-rust-history-peer"
(cd "$ROOT/go" && GOTOOLCHAIN=go1.25.14 CGO_ENABLED=0 go build -trimpath \
  -o "$PEER_BIN" ./tools/cadence-rust-history-peer) \
  > "$EVIDENCE_DIR/go-peer-build.log" 2>&1
rc=$?; record go-peer-build "$rc"
if [ "$rc" -eq 0 ]; then
  sha256sum "$PEER_BIN" | sed "s#$PEER_BIN#cadence-rust-history-peer#" > "$EVIDENCE_DIR/go-peer-binary.sha256"
  log "Go peer $(cat "$EVIDENCE_DIR/go-peer-binary.sha256")"
  if build_test_exe history-tchannel cadence-matching; then
    CADENCE_HISTORY_PEER_BIN="$PEER_BIN" run_exact_ignored history-tchannel cadence-matching "$GO_TEST" history-tchannel-test
    grep -q 'test .* ok' "$EVIDENCE_DIR/history-tchannel-test.log" || fail "history-tchannel-test: no ok line"
  fi
fi

########################################################################
# Oracle 2: live PostgreSQL TLS modes
########################################################################
log "== Oracle 2: live PostgreSQL TLS oracle"
S="$SECRET_DIR"
gen_ca() { # <prefix> <cn>
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=$2" \
    -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign \
    -keyout "$S/$1.key" -out "$S/$1.pem" 2>/dev/null
}
(
  set -e
  gen_ca ca "cadence-dep-oracle-ca"
  gen_ca wrong-ca "cadence-dep-oracle-wrong-ca"
  # Server certificate: CN/SAN DNS=localhost ONLY (deliberately no IP SAN).
  openssl req -new -newkey rsa:2048 -nodes -subj "/CN=localhost" \
    -keyout "$S/server.key" -out "$S/server.csr" 2>/dev/null
  printf 'subjectAltName=DNS:localhost\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n' > "$S/server.ext"
  openssl x509 -req -in "$S/server.csr" -CA "$S/ca.pem" -CAkey "$S/ca.key" -CAcreateserial \
    -days 1 -extfile "$S/server.ext" -out "$S/server.pem" 2>/dev/null
  openssl x509 -in "$S/server.pem" -noout -ext subjectAltName > "$EVIDENCE_DIR/server-cert-san.txt"
  # sslinline requires a client cert/key pair; the server does not request one.
  openssl req -new -newkey rsa:2048 -nodes -subj "/CN=cadence-dep-oracle-client" \
    -keyout "$S/client.key" -out "$S/client.csr" 2>/dev/null
  openssl x509 -req -in "$S/client.csr" -CA "$S/ca.pem" -CAkey "$S/ca.key" -CAcreateserial \
    -days 1 -out "$S/client.pem" 2>/dev/null
  mkdir "$S/pgcerts"
  cp "$S/server.pem" "$S/pgcerts/server.crt"
  cp "$S/server.key" "$S/pgcerts/server.key"
  chmod 600 "$S/pgcerts/server.key"
  chmod 755 "$S/pgcerts"
  sudo -n chown -R 999:999 "$S/pgcerts"
) > "$EVIDENCE_DIR/cert-gen.log" 2>&1
record cert-gen "$?"
if grep -qi 'IP Address' "$EVIDENCE_DIR/server-cert-san.txt"; then fail "server cert unexpectedly has an IP SAN"; fi

PORT="$(python3 -I -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])')"
docker run -d --name "$CONTAINER" --label cadence.dependency-oracle=disposable \
  --memory 1800m --memory-swap 1800m --cpus 1.5 --pids-limit 2048 \
  -e POSTGRES_PASSWORD -e POSTGRES_DB=cadence \
  -p "127.0.0.1:$PORT:5432" -v "$S/pgcerts:/certs:ro" "$PG_IMAGE" \
  -c ssl=on -c ssl_cert_file=/certs/server.crt -c ssl_key_file=/certs/server.key \
  > "$EVIDENCE_DIR/postgres-start.log" 2>&1
record postgres-start "$?"
ready=1
for _ in $(seq 1 60); do
  if docker exec "$CONTAINER" pg_isready -U postgres -d cadence >/dev/null 2>&1 \
     && [ "$(docker exec "$CONTAINER" psql -U postgres -d cadence -Atc 'SHOW ssl' 2>/dev/null)" = on ]; then ready=0; break; fi
  sleep 2
done
record postgres-ready "$ready"
docker inspect -f 'memory={{.HostConfig.Memory}} swap={{.HostConfig.MemorySwap}} nanocpus={{.HostConfig.NanoCpus}} pids={{.HostConfig.PidsLimit}}' \
  "$CONTAINER" > "$EVIDENCE_DIR/postgres-limits.txt" 2>&1

if [ "$ready" -eq 0 ] && build_test_exe postgres-tls cadence-persistence; then
  CA_INLINE="$(cat "$S/ca.pem")"
  WRONG_INLINE="$(cat "$S/wrong-ca.pem")"
  export CADENCE_POSTGRES_CERT_INLINE="$(cat "$S/client.pem")" CADENCE_POSTGRES_KEY_INLINE="$(cat "$S/client.key")"
  # case <name> <host> <sslmode> <ca-file|-> <inline-ca|-> <expect true|false> <marker>
  pg_case() {
    local name="$1" host="$2" mode="$3" cafile="$4" inline="$5" expect="$6" marker="$7"
    (
      export CADENCE_POSTGRES_ADDR="$host:$PORT" CADENCE_POSTGRES_USER=postgres \
        CADENCE_POSTGRES_DATABASE=cadence CADENCE_POSTGRES_SSLMODE="$mode" \
        CADENCE_POSTGRES_TLS_EXPECT_SUCCESS="$expect"
      [ "$cafile" = - ] || export CADENCE_POSTGRES_CA_FILE="$cafile"
      if [ "$inline" != - ]; then export CADENCE_POSTGRES_SSLINLINE=true CADENCE_POSTGRES_CA_INLINE="$inline"; fi
      run_exact_ignored postgres-tls cadence-persistence "$PG_TEST" "pgcase-$name"
    )
    local rc=$?
    [ "$rc" -eq 0 ] || OVERALL=1
    grep -q "$marker" "$EVIDENCE_DIR/pgcase-$name.log" || { fail "pgcase-$name: missing expected outcome marker '$marker'"; rc=1; }
    return "$rc"
  }
  OK="live PostgreSQL TLS case succeeded"
  REJ="live PostgreSQL TLS case rejected as expected"
  pg_case disable            127.0.0.1 disable   -          -               true  "$OK"
  pg_case require-no-ca      127.0.0.1 require   -          -               true  "$OK"
  pg_case verify-ca-valid    127.0.0.1 verify-ca "$S/ca.pem" -             true  "$OK"
  pg_case verify-full-local  localhost verify-full "$S/ca.pem" -           true  "$OK"
  pg_case verify-full-ip     127.0.0.1 verify-full "$S/ca.pem" -           false "$REJ"
  pg_case verify-ca-wrongca  127.0.0.1 verify-ca "$S/wrong-ca.pem" -      false "$REJ"
  pg_case require-wrongca    127.0.0.1 require   "$S/wrong-ca.pem" -      false "$REJ"
  pg_case inline-verify-ca   127.0.0.1 verify-ca -     "$CA_INLINE"       true  "$OK"
  pg_case inline-verify-full localhost verify-full -   "$CA_INLINE"       true  "$OK"
  pg_case inline-wrongca     127.0.0.1 verify-ca -     "$WRONG_INLINE"    false "$REJ"

  log "== Oracle 4: live PostgreSQL pool return wakeup"
  (
    export ORACLE_TIMEOUT=30s CADENCE_POSTGRES_ADDR="127.0.0.1:$PORT" CADENCE_POSTGRES_USER=postgres \
      CADENCE_POSTGRES_DATABASE=cadence
    run_exact_ignored postgres-tls cadence-persistence "$PG_POOL_TEST" postgres-pool-wakeup-test
  )
  rc=$?; record postgres-pool-wakeup-oracle "$rc"

  # The native store test uses a plain connection (the TLS cases above cover
  # connection policy). Seed the original Go schema, without Rust-only DDL.
  log "== Oracle 3: native PostgreSQL bytes, fencing and cross-read oracle"
  sha256sum "$ROOT/go/schema/postgres/cadence/schema.sql" > "$EVIDENCE_DIR/postgres-schema.sha256"
  docker exec -i "$CONTAINER" psql -U postgres -d cadence -v ON_ERROR_STOP=1 \
    < "$ROOT/go/schema/postgres/cadence/schema.sql" \
    > "$EVIDENCE_DIR/postgres-schema.log" 2>&1
  rc=$?; record postgres-schema "$rc"
  if [ "$rc" -eq 0 ]; then
    (
      export CADENCE_POSTGRES_ADDR="127.0.0.1:$PORT" CADENCE_POSTGRES_USER=postgres \
        CADENCE_POSTGRES_DATABASE=cadence
      run_exact_ignored postgres-tls cadence-persistence "$PG_NATIVE_TEST" postgres-native-test
    )
    rc=$?; record postgres-native-oracle "$rc"
  fi
fi

########################################################################
# Secret hygiene: nothing sensitive may remain in retained evidence.
########################################################################
if grep -rIl -e 'PRIVATE KEY' -e "$PG_PASSWORD" "$EVIDENCE_DIR" >/dev/null 2>&1; then
  grep -rIl -e 'PRIVATE KEY' -e "$PG_PASSWORD" "$EVIDENCE_DIR" > "$EVIDENCE_DIR/secret-hygiene.violations"
  fail "secret material found in evidence"
fi
printf '%s\n' "$OVERALL" > "$EVIDENCE_DIR/overall.exit"
log "overall exit $OVERALL"
exit "$OVERALL"
