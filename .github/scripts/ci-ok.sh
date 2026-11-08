#!/usr/bin/env bash
set -euo pipefail
jq -e -s '
  length == 1 and
  (.[0] | type == "object" and
    has("test") and has("quality") and has("dialyzer") and
    all(.[]; .result == "success"))
' >/dev/null
