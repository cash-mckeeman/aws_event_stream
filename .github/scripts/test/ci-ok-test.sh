#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/../../.." && pwd)"
gate="$root/.github/scripts/ci-ok.sh"
[ -f "$gate" ] || { echo 'missing CI result gate' >&2; exit 1; }
passed=0
check() {
  local want="$1" label="$2" input="$3" actual=1
  if printf '%s' "$input" | bash "$gate" >/dev/null 2>&1; then actual=0; fi
  [ "$actual" = "$want" ] || { echo "FAIL: $label" >&2; exit 1; }
  passed=$((passed + 1))
}
green='{"test":{"result":"success"},"quality":{"result":"success"},"dialyzer":{"result":"success"}}'
check 0 'all required jobs succeeded' "$green"
for job in test quality dialyzer; do
  for result in failure skipped cancelled timed_out action_required neutral pending; do
    input="$(jq -c --arg job "$job" --arg result "$result" '.[$job].result=$result' <<<"$green")"
    check 1 "$job $result cannot pass" "$input"
  done
  check 1 "missing $job cannot pass" "$(jq -c --arg job "$job" 'del(.[$job])' <<<"$green")"
  check 1 "missing $job result cannot pass" "$(jq -c --arg job "$job" '.[$job]={}' <<<"$green")"
done
check 1 'empty input cannot pass' ''
check 1 'malformed input cannot pass' '{'
check 1 'null input cannot pass' 'null'
check 1 'array input cannot pass' '[]'
check 1 'empty graph cannot pass' '{}'
check 1 'two objects cannot pass' "$green $green"
check 1 'additional failed job cannot pass' "$(jq -c '.extra={result:"failure"}' <<<"$green")"
check 0 'additional successful job is covered' "$(jq -c '.extra={result:"success"}' <<<"$green")"
# This structural guard makes a newly ungated workflow job fail CI.
ruby -ryaml -e '
  jobs = YAML.load_file(ARGV.fetch(0)).fetch("jobs")
  required = jobs.keys.reject { |key| key == "ci-ok" }.sort
  covered = jobs.fetch("ci-ok").fetch("needs").sort
  abort "ci-ok does not cover every workflow job" unless required == covered
' "$root/.github/workflows/ci.yml"
printf 'PASS: %s gate behavior cases and workflow coverage\n' "$passed"
