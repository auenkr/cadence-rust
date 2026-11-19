#!/usr/bin/env bash
# Worker helper for GitHub Actions runs (no local cargo/go/docker needed).
#
#   scripts/ci/gh-run.sh dispatch <workflow.yml> <ref> [key=value ...]   -> prints run id + URL (race-free)
#   scripts/ci/gh-run.sh wait <run-id>                                   -> blocks until done, prints per-job/step timings
#   scripts/ci/gh-run.sh summary <run-id> [--keep]                       -> downloads artifacts to a temp dir, prints
#                                                                          *.exit, summaries, benchmark p50/p99/CPU, failing
#                                                                          log tail; deletes the dir unless --keep
#   scripts/ci/gh-run.sh go <workflow.yml> <ref> [key=value ...]         -> dispatch + wait + summary
#
# Examples:
#   scripts/ci/gh-run.sh go rust-ci.yml my-branch package=cadence-history
#   scripts/ci/gh-run.sh go sdk-matching-benchmark.yml master service=history source_ref=<sha>
# Benchmarks: dispatch on master with source_ref=<sha> (no temp branch needed; caches saved on master are shared).
set -euo pipefail
REPO=${GH_REPO:-auenkr/cadence-rust}
TMP_ROOT=${CI_TMP_ROOT:-/mnt/storage/service/tmp}
export GH_REPO=$REPO

dispatch() {
  local wf=$1 ref=$2; shift 2
  local args=() kv out id
  for kv in "$@"; do args+=(-f "$kv"); done
  local since; since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  out=$(gh workflow run "$wf" --ref "$ref" "${args[@]}" 2>&1) || { echo "$out" >&2; return 1; }
  id=$(grep -oE 'actions/runs/[0-9]+' <<<"$out" | head -1 | cut -d/ -f3 || true)
  if [[ -z "$id" ]]; then
    # Fallback for older gh: newest workflow_dispatch run by me on this ref created after `since`.
    local me; me=$(gh api user -q .login)
    for _ in $(seq 1 20); do
      id=$(gh run list -w "$wf" -b "$ref" -e workflow_dispatch -u "$me" -L 5 --json databaseId,createdAt \
        -q "[.[]|select(.createdAt >= \"$since\")]|last|.databaseId // empty")
      [[ -n "$id" ]] && break; sleep 3
    done
  fi
  [[ -n "$id" ]] || { echo "could not resolve run id: $out" >&2; return 1; }
  echo "$id https://github.com/$REPO/actions/runs/$id"
}

timings() {
  gh run view "$1" --json name,conclusion,createdAt,jobs | python3 -I -c "$(cat <<'PY'
import json,sys,datetime as d
p=lambda s:d.datetime.fromisoformat(s.replace("Z","+00:00"))
r=json.load(sys.stdin); print("run:", r["name"], "->", r["conclusion"])
for j in r["jobs"]:
  if not (j.get("startedAt") and j.get("completedAt")): continue
  print("  job %s: %s queue %ds run %ds" % (j["name"], j["conclusion"], (p(j["startedAt"])-p(r["createdAt"])).seconds, (p(j["completedAt"])-p(j["startedAt"])).seconds))
  for s in j["steps"]:
    if s.get("startedAt") and s.get("completedAt"):
      t=(p(s["completedAt"])-p(s["startedAt"])).seconds
      if t>=15 or s["conclusion"]=="failure": print("    %5ds %-9s %s" % (t, s["conclusion"], s["name"]))
PY
)"
}

wait_run() {
  local id=$1 st
  while :; do
    st=$(gh run view "$id" --json status -q .status)
    [[ "$st" == completed ]] && break
    sleep 60
  done
  timings "$id"
  [[ "$(gh run view "$id" --json conclusion -q .conclusion)" == success ]]
}

summary() {
  local id=$1 keep=${2:-}
  mkdir -p "$TMP_ROOT"; local dir; dir=$(mktemp -d "$TMP_ROOT/run-$id-XXXX")
  gh run download "$id" -D "$dir" >/dev/null 2>&1 || echo "(no artifacts)"
  echo "artifacts: $dir"
  find "$dir" -name '*.exit' -size -64c | sort | while read -r f; do printf '  %s = %s\n' "${f#$dir/}" "$(tr -d '\n' < "$f")"; done
  find "$dir" \( -iname '*summary*' -o -name 'verdict*' -o -name '*.sha256' \) -type f -size -64k | sort | while read -r f; do
    echo "--- ${f#$dir/}"; head -c 3000 "$f"; echo
  done
  # Benchmark metrics (p50/p99/CPU) from any JSON result.
  find "$dir" -name '*.json' -size -2M -print0 | xargs -0 -r python3 -I -c '
import json,sys
keys=("p50","p95","p99","cpu","throughput","rate","errors")
def walk(o,path):
  if isinstance(o,dict):
    for k,v in o.items(): walk(v,path+[str(k)])
  elif isinstance(o,list):
    for i,v in enumerate(o[:50]): walk(v,path+[str(i)])
  elif isinstance(o,(int,float)) and any(k in path[-1].lower() for k in keys):
    print("   ",".".join(path),"=",o)
for f in sys.argv[1:]:
  try: o=json.load(open(f))
  except Exception: continue
  print("  metrics", f.split("/")[-2]+"/"+f.split("/")[-1]); walk(o,[])' 2>/dev/null | head -200
  if [[ "$(gh run view "$id" --json conclusion -q .conclusion)" != success ]]; then
    echo "--- failed step log tail"; gh run view "$id" --log-failed 2>/dev/null | tail -60
  fi
  [[ "$keep" == --keep ]] || rm -rf "$dir"
}

cmd=${1:-}; shift || true
case "$cmd" in
  dispatch) dispatch "$@" ;;
  wait) wait_run "$1" ;;
  timings) timings "$1" ;;
  summary) summary "$@" ;;
  go) line=$(dispatch "$@"); echo "$line"; id=${line%% *}; rc=0; wait_run "$id" || rc=$?; summary "$id"; exit "$rc" ;;
  *) sed -n '2,14p' "$0"; exit 2 ;;
esac
