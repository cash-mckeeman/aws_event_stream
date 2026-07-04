# Upstream Golden Vectors: Sync Task + Drift Watcher

**Date:** 2026-07-04
**Status:** Approved
**Context:** README "Future work" and `test/support/fixtures.ex` both call for
externally-sourced golden vectors from an upstream AWS SDK corpus (originally
flagged as watcher work in Linear MIM-43). This spec adds that corpus, a mix
task to refresh it, and a scheduled CI watcher that opens a GitHub issue when
the upstream corpus drifts.

## Goals

1. Verify interoperability against vectors AWS itself tests with — not just
   self-encoded regression locks.
2. Make refreshing those vectors a one-command operation.
3. Detect upstream corpus changes automatically and surface them as a GitHub
   issue that says whether our codec still passes.

## Non-goals

- Auto-committing synced fixtures from CI (a maintainer reviews and commits).
- Scraping vector data embedded in botocore/aws-c-event-stream test source.
  The sync core is structured so a second source can be added later.
- Replacing the existing self-encoded vectors or the captured Bedrock frame —
  the SDK corpus is additive.

## Upstream source

`aws/aws-sdk-go-v2`, path `aws/protocol/eventstream/testdata/`, branch `main`.
The only AWS corpus published as consumable data files. Layout (9 cases today):

- `encoded/positive/<case>` — raw binary event-stream frames (5 cases:
  `all_headers`, `empty_message`, `int32_header`, `payload_no_headers`,
  `payload_one_str_header`)
- `encoded/negative/<case>` — deliberately corrupted frames (4 cases:
  `corrupted_header_len`, `corrupted_headers`, `corrupted_length`,
  `corrupted_payload`)
- `decoded/positive/<case>` — JSON: `total_length`, `headers_length`,
  `prelude_crc`, `message_crc` (signed int32), `headers` (list of
  `{name, type, value}` where `type` is the wire type byte; `bytes`/`string`/
  `uuid` values are base64), `payload` (base64)
- `decoded/negative/<case>` — plain-text expected error, e.g.
  "Prelude checksum mismatch", "Message checksum mismatch"

## 0. Decoder prerequisite: validate the prelude CRC before waiting for the frame

Discovered by running the upstream negative vectors against the current
decoder: `corrupted_length` (total_length inflated 61 → 62, file is 61 bytes)
never surfaces an error — the decoder checks `byte_size(buffer) < total`
*before* validating the prelude CRC, so it waits forever for bytes that never
come. Go validates the prelude CRC as soon as the 12-byte prelude is read.

**Change to `AWSEventStream.Decoder`:** once 12 bytes are available, validate
the prelude CRC first, then the `total_length >= 16` floor, then wait for the
full frame. On prelude-CRC failure the `total_length` field cannot be trusted
to bound the frame, so the error attaches the **entire remaining buffer** as
its raw bytes and decoding stops (`rest = <<>>`) — consistent with the
existing too-small-total behavior and the "resync is the caller's problem"
non-goal.

Two existing decoder tests hand-build preludes with bogus CRCs and rely on the
old check order (`too-small total_len`, `negative body_len`); they are updated
to carry valid prelude CRCs so they still exercise the length checks.

## 1. Fixture layout

```
test/fixtures/aws_sdk_go_v2/
  manifest.json
  encoded/positive/<case>
  encoded/negative/<case>
  decoded/positive/<case>
  decoded/negative/<case>
```

Files are byte-for-byte verbatim copies of upstream. `manifest.json` records:

```json
{
  "source": {
    "repo": "aws/aws-sdk-go-v2",
    "path": "aws/protocol/eventstream/testdata",
    "ref": "main",
    "commit": "<sha of upstream commit at sync time>"
  },
  "files": { "<relative path>": "<sha256 hex>", ... }
}
```

No timestamps in the manifest (avoids churn on no-op syncs).

## 2. Mix task: `mix aws_event_stream.sync_fixtures`

**File:** `lib/mix/tasks/aws_event_stream.sync_fixtures.ex`

**Behavior:**

1. Resolve upstream HEAD: GitHub API `GET /repos/aws/aws-sdk-go-v2/commits/main`
   → commit SHA.
2. List corpus files: recursive walk of the contents API
   (`GET /repos/.../contents/<path>?ref=<sha>`) starting at the testdata dir —
   a recursive tree fetch of the whole aws-sdk-go-v2 repo would risk the tree
   API's truncation limits; the contents walk is ~7 small calls.
3. Download each file from `raw.githubusercontent.com/aws/aws-sdk-go-v2/<sha>/<path>`.
4. Compute changeset vs. the local fixture dir: added / changed / removed files
   (byte comparison), plus upstream SHA old → new.
5. Write updated files, delete removed ones, regenerate `manifest.json`.
6. Print a human-readable summary of the changeset.
7. **Exit 0 if nothing changed; exit 2 (`exit({:shutdown, 2})`) if anything
   changed.** The exit code is the CI drift signal. Drift uses 2, not 1,
   because `Mix.raise` (any task failure) already exits 1 — the watcher must
   distinguish "corpus drifted" from "task crashed" (which fails the workflow).

**Structure — pure core, thin shell:**

- `fetch/0` — HTTP side effects only; returns `{upstream_sha, %{rel_path => binary}}`.
- `changeset(fetched_files, local_dir)` — pure; returns
  `%{added: [...], changed: [...], removed: [...]}`.
- `apply_changeset/manifest generation` — filesystem writes, driven by the
  changeset and fetched map.
- The Mix task's `run/1` wires these together and formats the summary.

**HTTP client:** `:httpc` (+ `:ssl`) from OTP — zero new dependencies (chosen
over Req to keep the minimal lib's mix.lock free of finch/mint/etc. for a
maintainer-only task). `:httpc` does **not** verify TLS peers by default, so
every request MUST pass explicit ssl options: `verify: :verify_peer`,
`cacerts: :public_key.cacerts_get()`, `depth: 3`, and default hostname
matching. `:public_key.cacerts_get/0` requires OTP 25+ — acceptable because
only maintainers/CI run the task (CI uses OTP 27); the library itself keeps
its existing version floor. If the `GITHUB_TOKEN` env var is set, send it as
a bearer token on the two API calls (rate-limit headroom in CI);
raw.githubusercontent downloads are unauthenticated.

**Packaging:** the task must not ship in the hex package. `mix.exs` `package.files`
narrows from `~w(lib ...)` to `~w(lib/aws_event_stream lib/aws_event_stream.ex
mix.exs README.md LICENSE)`.

## 3. Test integration: `test/aws_event_stream/sdk_vectors_test.exs`

Walks `test/fixtures/aws_sdk_go_v2/` at test time — no hardcoded case list, so
newly synced cases are exercised automatically. A missing or empty fixture dir
fails the test with a message pointing at the sync task — never a silent pass.

**Positive cases** — for each `encoded/positive/<case>`:

- Build expected `%Message{}` from `decoded/positive/<case>` JSON:
  - Type byte → header atom: 0/1 → `:bool` (0 = true, 1 = false), 2 → `:byte`,
    3 → `:short`, 4 → `:integer`, 5 → `:long`, 6 → `:bytes` (base64-decode),
    7 → `:string` (base64-decode), 8 → `:timestamp`, 9 → `:uuid` (base64-decode).
  - Payload: base64-decode.
- Assert `Decoder.decode(frame)` yields exactly `{[{:ok, expected}], <<>>}`.
- Assert `Encoder.encode(expected)` re-produces the frame byte-for-byte.
- Assert the frame's embedded `prelude_crc` / `message_crc` equal the JSON's
  values normalized from signed to unsigned int32 (upstream serializes Go
  int32; e.g. `all_headers` has `"message_crc": -1415188212`).

**Negative cases** — for each `encoded/negative/<case>`:

- Map `decoded/negative/<case>` prose → expected error atom:
  - "Prelude checksum mismatch" → `:invalid_prelude_crc` (covers
    `corrupted_header_len` and — via the section-0 decoder change —
    `corrupted_length`)
  - "Message checksum mismatch" → `:invalid_message_crc`
- Assert decode yields `{[{:error, {expected_atom, _raw}}], _rest}`.
- An unmapped prose string **fails the test** with a message naming the new
  string — that is the intended signal that upstream added a corruption mode
  we haven't classified.

The JSON→Message builder and prose→atom map live in `AWSEventStream.Fixtures`
(`test/support/fixtures.ex`) so the task's tests and the vectors test share them.

## 4. Watcher workflow: `.github/workflows/fixtures-watch.yml`

- **Triggers:** `schedule` (weekly, Monday 06:00 UTC) + `workflow_dispatch`.
- **Permissions:** `issues: write`, `contents: read`.
- **Steps:**
  1. Checkout, `erlef/setup-beam` (same versions as ci.yml), `mix deps.get`.
  2. Run `mix aws_event_stream.sync_fixtures`, capturing stdout and exit code.
  3. Exit 0 → done (green, no action). Any exit other than 0/2 → fail the
     workflow (task crashed).
  4. Exit 2 → run `mix test` against the freshly synced fixtures; record
     pass/fail.
  5. Report via `gh` (using `GITHUB_TOKEN`):
     - If an **open** issue labeled `upstream-fixtures-drift` exists → add a
       comment with the new report.
     - Otherwise → `gh issue create` with label `upstream-fixtures-drift`.
     - Body: sync summary (added/changed/removed), old → new upstream SHA with
       a compare link, and a headline line stating whether `mix test` **passed
       or failed** against the new vectors.
- The workflow never commits. The issue is the prompt for a maintainer to run
  the sync task locally, review, and commit.
- The `upstream-fixtures-drift` label is created once, manually (or by the
  workflow with `gh label create --force`).

## 5. Docs & housekeeping

- README: drop the "Externally-sourced golden vectors" future-work bullet; add
  a short "Upstream test vectors" section (provenance, sync task usage, watcher
  behavior); update the status line's test count.
- Update the `test/support/fixtures.ex` comment that points at MIM-43 as future
  work.

## Testing strategy

- **No network in the test suite.** The sync task's HTTP layer is not unit
  tested; everything after fetch is.
- Unit tests (tmp dirs) for: `changeset/2` (added/changed/removed detection),
  manifest generation, and changeset application (writes + deletions).
- Unit tests for the decoded-JSON→Message builder (all 9 header types, base64
  handling, signed→unsigned CRC normalization) and the prose→atom map
  (including the unknown-prose failure path).
- `sdk_vectors_test.exs` runs against the committed corpus in normal `mix test`.

## Error handling

- Sync task: any HTTP failure, unexpected API shape, or empty file list →
  `Mix.raise` with the underlying reason; never write a partial corpus (compute
  the full fetched map first, then apply).
- Vectors test: missing decoded counterpart for an encoded case (or vice versa)
  fails the test naming the orphaned file.

## Acceptance criteria

1. `mix aws_event_stream.sync_fixtures` on a clean checkout downloads the
   corpus, writes `test/fixtures/aws_sdk_go_v2/` + manifest, exits 2 (first
   run is all-added); a second immediate run exits 0 with "no changes".
2. `mix test` passes with the committed corpus, exercising all 5 positive and
   4 negative upstream cases — including `corrupted_length`, which requires
   the section-0 decoder change.
3. Tampering with a local fixture byte then running the sync task reports that
   file as changed and exits 2.
4. `fixtures-watch.yml` is green on a no-drift run and opens/comments on a
   labeled issue (with test pass/fail headline) on a drift run.
5. Hex package contents exclude `lib/mix/`.
