#!/bin/bash
# Rust image replacement for Go `cadence-server start --auto-setup` schema setup.
# Uses the cadence-sql-tool / cadence-cassandra-tool that ship in the Go base image
# (no Go cadence-server binary needed). Driven by the same env as config_template.yaml.
set -eu

DB="$(echo "${DB:-cassandra}" | tr '[:upper:]' '[:lower:]')"
SCHEMA_DIR="${SCHEMA_DIR:-/etc/cadence/schema}"
RF="${CADENCE_SETUP_OPTION_REPLICATION_FACTOR:-${CASSANDRA_REPLICATION_FACTOR:-1}}"

retry() { # retry <attempts> cmd...
  local n=$1; shift
  for ((i = 1; i <= n; i++)); do "$@" && return 0; echo "retry $i/$n: $*" >&2; sleep 2; done
  return 1
}

case "$DB" in
  cassandra | scylla)
    ep="${CASSANDRA_SEEDS:?CASSANDRA_SEEDS required}"
    port="${CASSANDRA_PORT:-9042}"
    auth=()
    [ -n "${CASSANDRA_USER:-}" ] && auth+=(--user "$CASSANDRA_USER" --password "${CASSANDRA_PASSWORD:-}")
    t() { cadence-cassandra-tool --ep "$ep" -p "$port" --protocol-version "${CASSANDRA_PROTO_VERSION:-4}" "${auth[@]}" "$@"; }
    for pair in "${KEYSPACE:-cadence}:cadence" "${VISIBILITY_KEYSPACE:-cadence_visibility}:visibility"; do
      ks="${pair%%:*}"; kind="${pair##*:}"
      retry 60 t create -k "$ks" --rf "$RF"
      t -k "$ks" setup-schema -v 0.0 || true
      t -k "$ks" update-schema -d "$SCHEMA_DIR/cassandra/$kind/versioned"
    done
    ;;
  mysql | postgres)
    if [ "$DB" = mysql ]; then
      ep="${MYSQL_SEEDS:?MYSQL_SEEDS required}"; port="${DB_PORT:-3306}"
      user="${MYSQL_USER:-}"; pw="${MYSQL_PWD:-}"; sdir="$SCHEMA_DIR/mysql/v8"
    else
      ep="${POSTGRES_SEEDS:?POSTGRES_SEEDS required}"; port="${DB_PORT:-5432}"
      user="${POSTGRES_USER:-}"; pw="${POSTGRES_PWD:-}"; sdir="$SCHEMA_DIR/postgres"
    fi
    t() { cadence-sql-tool --plugin "$DB" --ep "$ep" -p "$port" -u "$user" --pw "$pw" "$@"; }
    for pair in "${DBNAME:-cadence}:cadence" "${VISIBILITY_DBNAME:-cadence_visibility}:visibility"; do
      name="${pair%%:*}"; kind="${pair##*:}"
      retry 60 t create-database --db "$name"
      t --db "$name" setup-schema -v 0.0 || true
      t --db "$name" update-schema -d "$sdir/$kind/versioned"
    done
    ;;
  *) echo "auto-setup: unsupported DB=$DB" >&2; exit 1 ;;
esac

# Go auto-setup also creates the "default" domain. Register it through the CLI once
# a local Frontend is serving (only when this container runs the frontend service).
case ",${SERVICES:-history,matching,frontend,worker}," in
  *,frontend,*)
    (
      addr="${HOST_IP:-127.0.0.1}:${FRONTEND_GRPC_PORT:-7833}"
      for _ in $(seq 1 120); do
        if cadence --address "$addr" --transport grpc --domain default domain describe >/dev/null 2>&1; then exit 0; fi
        if cadence --address "$addr" --transport grpc --domain default domain register --rd 7 --desc "Example Domain" >/dev/null 2>&1; then exit 0; fi
        sleep 3
      done
    ) >/tmp/default-domain.log 2>&1 &
    ;;
esac
