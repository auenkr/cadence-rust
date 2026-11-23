#!/bin/sh
# /start-cadence.sh runs `cadence-server ... start --services=$SERVICES`. The Rust History service is a
# separate binary (cadence-history, same CLI); route SERVICES=history to it, everything else to cadence-server.
for a in "$@"; do
  case "$a" in
    --services=history|--services=history,*|--services=*,history|--services=*,history,*) exec /usr/local/bin/cadence-history "$@" ;;
  esac
done
exec /usr/local/bin/cadence-server-real "$@"
