#!/usr/bin/env bash
# Structural: the lines of publish.yml that decide who may publish to Hex.
set -u
WF="$(cd "$(dirname "$0")/../.." && pwd)/workflows/publish.yml"
fail=0
check() { if [ "$2" = "$3" ]; then echo "ok: $1"; else echo "FAIL: $1 (want '$2', got '$3')"; fail=1; fi; }
# job <name>: the lines of that job, up to the next top-level job.
job() { awk -v n="  $1:" '$0 == n {f=1; next} f && /^  [a-z_-]+:/ {exit} f' "$WF"; }
# before <a> <b>: 1 when the first line matching <a> in the publish job comes before the first matching <b>.
before() { local a b; a="$(job publish | grep -nF -- "$1" | head -1 | cut -d: -f1)"; b="$(job publish | grep -nF -- "$2" | head -1 | cut -d: -f1)"
  [ -n "$a" ] && [ -n "$b" ] && [ "$a" -lt "$b" ] && echo 1 || echo 0; }

check "the publish job exists" 1 "$([ -n "$(job publish)" ] && echo 1 || echo 0)"
check "no line carries a Hex key" 0 "$(grep -c 'HEX_API_KEY' "$WF")"
check "no line reads a secret" 0 "$(grep -v '^ *#' "$WF" | grep -c 'secrets\.')"
check "id-token: write appears once in the file" 1 "$(grep -c 'id-token: write' "$WF")"
check "the publish job holds it" 1 "$(job publish | grep -c 'id-token: write')"
check "the publish job keeps contents: read" 1 "$(job publish | grep -c '      contents: read')"
check "the publish job runs in prod, where hex.pm trusts it" 1 "$(job publish | grep -c '    environment: prod')"
check "the publish job runs the Hex version check" 1 "$(job publish | grep -c 'run: bash .github/scripts/hex-workload-identity.sh')"
check "the Hex version check follows setup-beam" 1 "$(before 'uses: erlef/setup-beam' 'hex supports workload identity')"
check "the Hex version check precedes mix hex.publish" 1 "$(before 'hex supports workload identity' 'mix hex.publish')"
exit $fail
