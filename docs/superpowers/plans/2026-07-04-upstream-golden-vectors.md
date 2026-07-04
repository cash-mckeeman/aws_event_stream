# Upstream Golden Vectors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Import the aws-sdk-go-v2 event-stream test corpus as committed fixtures, add a `mix aws_event_stream.sync_fixtures` task to refresh them, and a scheduled GitHub Actions watcher that opens/updates an issue when the upstream corpus drifts.

**Architecture:** Fixtures are verbatim copies of `aws/aws-sdk-go-v2:aws/protocol/eventstream/testdata/` under `test/fixtures/aws_sdk_go_v2/` plus a `manifest.json` pinning the upstream SHA. The sync task is a pure changeset core with a thin `:httpc` fetch shell; exit code 2 signals drift to CI. A prerequisite decoder fix (validate prelude CRC as soon as 12 bytes arrive) is required for the `corrupted_length` negative vector to pass.

**Tech Stack:** Elixir/OTP only — `:httpc`/`:ssl` for HTTP, Jason (already a dev/test dep) for JSON, ExUnit, GitHub Actions + `gh` CLI for the watcher.

**Spec:** `docs/superpowers/specs/2026-07-04-upstream-golden-vectors-design.md`

## Global Constraints

- **No new deps.** HTTP via `:httpc`; JSON via Jason (already in mix.lock, optional for consumers).
- **TLS must be verified** on every `:httpc` request: `verify: :verify_peer`, `cacerts: :public_key.cacerts_get()`, `depth: 3`, `customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]`.
- **Exit codes:** 0 = no drift, 2 = drift (`exit({:shutdown, 2})`), anything else = task failure (`Mix.raise` exits 1).
- **Version control is jj, never git.** Commit = `jj describe -m "<msg>"` then `jj new`. Diffs/log always with `--git`.
- **This is multi-commit work: execute in a jj workspace** created via the `EnterWorktree` tool (per `~/.claude/CLAUDE.md`), NOT in the default workspace.
- Run `mix format` before every commit (CI enforces `mix format --check-formatted`).
- Library version floor stays `elixir: ">= 1.15.0"`; the sync task may require OTP 25+ at *runtime* (`:public_key.cacerts_get/0`) — documented, not enforced in code.
- Upstream constants: repo `aws/aws-sdk-go-v2`, path `aws/protocol/eventstream/testdata`, ref `main`.

---

### Task 1: Decoder — validate the prelude CRC as soon as the prelude arrives

The upstream `corrupted_length` vector (total_length inflated 61→62, buffer is 61 bytes) currently yields `{[], buffer}` forever — the decoder checks buffer completeness before prelude integrity. Reorder: prelude CRC first (once 12 bytes exist), then the length floor, then wait for the full frame. On prelude-CRC failure the length field is untrusted, so attach the whole remaining buffer as raw bytes and stop with `rest = <<>>`.

**Files:**
- Modify: `lib/aws_event_stream/decoder.ex`
- Test: `test/aws_event_stream/decoder_test.exs`

**Interfaces:**
- Consumes: nothing new.
- Produces: unchanged public API `Decoder.decode(binary, keyword) :: {[{:ok, Message.t()} | {:error, {atom(), binary()}}], binary()}`. New observable behavior: a ≥12-byte buffer with an invalid prelude CRC yields `{[... {:error, {:invalid_prelude_crc, whole_remaining_buffer}}], <<>>}` even when `byte_size(buffer) < total_length`.

- [ ] **Step 1: Write the failing test**

Append to `test/aws_event_stream/decoder_test.exs` (inside the module):

```elixir
test "a truncated buffer whose corrupt prelude claims more bytes errors immediately" do
  good = frame(%Message{headers: [], payload: "hello"})
  # Inflate total_length without fixing the prelude CRC: the buffer now looks
  # like an incomplete frame, but the prelude is provably corrupt. Upstream's
  # corrupted_length vector is exactly this shape.
  <<total::big-32, rest::binary>> = good
  corrupt = <<total + 1::big-32, rest::binary>>
  assert {[{:error, {:invalid_prelude_crc, ^corrupt}}], <<>>} = Decoder.decode(corrupt)
end
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mix test test/aws_event_stream/decoder_test.exs`
Expected: 1 failure — `match (=) failed` with left `{[{:error, {:invalid_prelude_crc, ...}}], ""}` vs right `{[], <<...>>}` (decoder returned no results, whole buffer as rest).

- [ ] **Step 3: Fix the two existing tests that rely on the old check order**

Both hand-build preludes with a bogus CRC of 0; under the new order they'd hit `:invalid_prelude_crc` before the length checks they exist to exercise. Give them valid prelude CRCs.

Replace the test `"negative body_len from a malformed headers_len yields an error, not a crash"` body with:

```elixir
test "negative body_len from a malformed headers_len yields an error, not a crash" do
  # total=20 (>=16, passes the length guard); hlen=10 -> body_len = 20-12-10-4 = -6
  # Prelude CRC is valid so the length check is what's exercised; buffer carries
  # exactly `total` bytes so we reach the body split branch.
  prelude = <<20::big-32, 10::big-32>>
  buffer = prelude <> <<:erlang.crc32(prelude)::big-32>> <> :binary.copy(<<0>>, 8)
  assert {[{:error, {:invalid_message_length, ^buffer}}], <<>>} = Decoder.decode(buffer)
end
```

Replace the test `"a too-small total_len yields :invalid_message_length"` body with:

```elixir
test "a too-small total_len with a valid prelude CRC yields :invalid_message_length" do
  prelude = <<10::big-32, 0::big-32>>
  buffer = prelude <> <<:erlang.crc32(prelude)::big-32>>
  assert {[{:error, {:invalid_message_length, _}}], <<>>} = Decoder.decode(buffer)
end
```

- [ ] **Step 4: Implement the reordered decode loop**

In `lib/aws_event_stream/decoder.ex`, replace the second `decode_loop/2` clause and `verify/7` with:

```elixir
defp decode_loop(<<total::big-32, hlen::big-32, pcrc::big-32, rest::binary>> = buffer, acc) do
  cond do
    :erlang.crc32(<<total::big-32, hlen::big-32>>) != pcrc ->
      # The prelude fails its own checksum, so total_length cannot be trusted
      # to bound the frame — attach the whole remaining buffer and stop.
      {Enum.reverse([{:error, {:invalid_prelude_crc, buffer}} | acc]), <<>>}

    total < @min_size ->
      {Enum.reverse([{:error, {:invalid_message_length, buffer}} | acc]), <<>>}

    byte_size(buffer) < total ->
      # whole frame not yet available — hand the buffer back for the next chunk
      {Enum.reverse(acc), buffer}

    true ->
      body_len = total - @prelude_size - hlen - @crc_size
      frame = binary_part(buffer, 0, total)

      if body_len < 0 do
        tail = binary_part(buffer, total, byte_size(buffer) - total)
        decode_loop(tail, [{:error, {:invalid_message_length, frame}} | acc])
      else
        <<headers_bin::binary-size(^hlen), payload::binary-size(^body_len), mcrc::big-32,
          tail::binary>> = rest

        result = verify(total, headers_bin, payload, mcrc, frame)
        decode_loop(tail, [result | acc])
      end
  end
end

defp verify(total, headers_bin, payload, mcrc, frame) do
  if :erlang.crc32(binary_part(frame, 0, total - @crc_size)) != mcrc do
    {:error, {:invalid_message_crc, frame}}
  else
    case safe_decode_headers(headers_bin) do
      {:ok, headers} -> {:ok, %Message{headers: headers, payload: payload}}
      :error -> {:error, {:invalid_headers, frame}}
    end
  end
end
```

Update the moduledoc's error paragraph to mention the early check — replace the sentence beginning "Possible error reasons:" paragraph's lead-in with:

```
Possible error reasons: `:invalid_prelude_crc`, `:invalid_message_crc`,
`:invalid_message_length`, and `:invalid_headers` (the header block could not
be parsed, e.g. an unknown header type or a value whose declared length runs
past the frame). The prelude CRC is validated as soon as the 12-byte prelude
is available — a corrupt prelude is reported immediately rather than waiting
for a (untrustworthy) declared frame length to arrive; since the frame bounds
are then unknowable, the error carries the whole remaining buffer and decoding
stops. Malformed input is always surfaced as an error, never raised.
```

- [ ] **Step 5: Run the full suite**

Run: `mix test`
Expected: all tests pass (27 tests — the 26 existing plus the new one — 0 failures).

- [ ] **Step 6: Format and commit**

```bash
mix format
jj describe -m "fix(decoder): validate prelude CRC before waiting for the declared frame length

A corrupt total_length that inflates the frame size previously made decode/2
report an incomplete frame forever. Matches aws-sdk-go-v2, whose corrupted_length
test vector this change is required for."
jj new
```

---

### Task 2: Sync-task pure core — changeset, manifest, apply

Pure, network-free functions of the sync task, TDD'd against tmp dirs. The Mix task module exists after this task but has no `run/1` yet.

**Files:**
- Create: `lib/mix/tasks/aws_event_stream.sync_fixtures.ex`
- Test: `test/mix/tasks/aws_event_stream.sync_fixtures_test.exs`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces (used by Task 3's `run/1`):
  - `changeset(fetched :: %{String.t() => binary()}, dir :: Path.t()) :: %{added: [String.t()], changed: [String.t()], removed: [String.t()]}` — keys are corpus-relative paths (e.g. `"encoded/positive/all_headers"`); lists sorted; `manifest.json` never appears in any list.
  - `apply_sync(fetched, changeset, commit_sha :: String.t(), dir) :: :ok` — writes added+changed, deletes removed, always (re)writes `manifest.json`.
  - `local_manifest_commit(dir) :: String.t() | nil`
  - `summary(changeset, old_sha :: String.t() | nil, new_sha :: String.t()) :: String.t()`
  - `manifest_json(fetched, commit_sha) :: String.t()` (deterministic, sorted keys, trailing newline)

- [ ] **Step 1: Write the failing tests**

Create `test/mix/tasks/aws_event_stream.sync_fixtures_test.exs`:

```elixir
defmodule Mix.Tasks.AwsEventStream.SyncFixturesTest do
  use ExUnit.Case, async: true
  alias Mix.Tasks.AwsEventStream.SyncFixtures

  @sha "aabbccddeeff00112233445566778899aabbccdd"

  setup do
    dir = Path.join(System.tmp_dir!(), "sync_fixtures_test_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, dir: dir}
  end

  defp seed(dir, files) do
    for {path, bin} <- files do
      dest = Path.join(dir, path)
      File.mkdir_p!(Path.dirname(dest))
      File.write!(dest, bin)
    end
  end

  describe "changeset/2" do
    test "a missing local dir means everything is added", %{dir: dir} do
      fetched = %{"encoded/positive/a" => "1", "decoded/positive/a" => "2"}

      assert SyncFixtures.changeset(fetched, dir) == %{
               added: ["decoded/positive/a", "encoded/positive/a"],
               changed: [],
               removed: []
             }
    end

    test "detects changed and removed files, ignores identical ones", %{dir: dir} do
      seed(dir, %{"encoded/positive/same" => "x", "encoded/positive/old" => "1", "encoded/negative/gone" => "z"})
      fetched = %{"encoded/positive/same" => "x", "encoded/positive/old" => "2", "encoded/positive/new" => "n"}

      assert SyncFixtures.changeset(fetched, dir) == %{
               added: ["encoded/positive/new"],
               changed: ["encoded/positive/old"],
               removed: ["encoded/negative/gone"]
             }
    end

    test "manifest.json is never part of the changeset", %{dir: dir} do
      seed(dir, %{"manifest.json" => "{}", "encoded/positive/a" => "1"})
      fetched = %{"encoded/positive/a" => "1"}
      assert SyncFixtures.changeset(fetched, dir) == %{added: [], changed: [], removed: []}
    end
  end

  describe "apply_sync/4 + manifest" do
    test "writes added/changed, removes removed, and pins the manifest", %{dir: dir} do
      seed(dir, %{"encoded/positive/old" => "1", "encoded/negative/gone" => "z"})
      fetched = %{"encoded/positive/old" => "2", "encoded/positive/new" => "n"}
      cs = SyncFixtures.changeset(fetched, dir)

      assert :ok = SyncFixtures.apply_sync(fetched, cs, @sha, dir)

      assert File.read!(Path.join(dir, "encoded/positive/old")) == "2"
      assert File.read!(Path.join(dir, "encoded/positive/new")) == "n"
      refute File.exists?(Path.join(dir, "encoded/negative/gone"))

      manifest = dir |> Path.join("manifest.json") |> File.read!() |> Jason.decode!()
      assert manifest["source"]["repo"] == "aws/aws-sdk-go-v2"
      assert manifest["source"]["path"] == "aws/protocol/eventstream/testdata"
      assert manifest["source"]["commit"] == @sha
      assert manifest["files"]["encoded/positive/new"] == Base.encode16(:crypto.hash(:sha256, "n"), case: :lower)
      assert Map.keys(manifest["files"]) |> Enum.sort() == ["encoded/positive/new", "encoded/positive/old"]

      # a re-run against the applied state is a no-op
      assert SyncFixtures.changeset(fetched, dir) == %{added: [], changed: [], removed: []}
      assert SyncFixtures.local_manifest_commit(dir) == @sha
    end

    test "manifest_json is deterministic and newline-terminated" do
      # zzz- prefix keeps the keys from colliding with source-block text when
      # asserting serialization order below
      fetched = %{"zzz-b" => "2", "zzz-a" => "1"}
      json = SyncFixtures.manifest_json(fetched, @sha)
      assert json == SyncFixtures.manifest_json(fetched, @sha)
      assert String.ends_with?(json, "\n")
      # sorted file keys: zzz-a serialized before zzz-b
      assert :binary.match(json, ~s("zzz-a")) < :binary.match(json, ~s("zzz-b"))
    end
  end

  test "local_manifest_commit is nil when absent or unparseable", %{dir: dir} do
    assert SyncFixtures.local_manifest_commit(dir) == nil
    seed(dir, %{"manifest.json" => "not json"})
    assert SyncFixtures.local_manifest_commit(dir) == nil
  end

  test "summary names every file and both shas" do
    cs = %{added: ["encoded/positive/new"], changed: ["decoded/positive/old"], removed: ["encoded/negative/gone"]}
    s = SyncFixtures.summary(cs, "1111111111111111", "2222222222222222")
    assert s =~ "111111111111 -> 222222222222"
    assert s =~ "added:   encoded/positive/new"
    assert s =~ "changed: decoded/positive/old"
    assert s =~ "removed: encoded/negative/gone"
    assert s =~ "https://github.com/aws/aws-sdk-go-v2/compare/1111111111111111...2222222222222222"
    # first sync has no prior sha and no compare link
    first = SyncFixtures.summary(cs, nil, "2222222222222222")
    assert first =~ "(none) -> 222222222222"
    refute first =~ "/compare/"
  end
end
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test test/mix/tasks/aws_event_stream.sync_fixtures_test.exs`
Expected: compile error — `Mix.Tasks.AwsEventStream.SyncFixtures` is undefined.

- [ ] **Step 3: Implement the pure core**

Create `lib/mix/tasks/aws_event_stream.sync_fixtures.ex`:

```elixir
defmodule Mix.Tasks.AwsEventStream.SyncFixtures do
  @shortdoc "Sync test/fixtures/aws_sdk_go_v2 from the upstream aws-sdk-go-v2 corpus"
  @moduledoc """
  Maintainer task: mirrors `aws/aws-sdk-go-v2:aws/protocol/eventstream/testdata`
  into `test/fixtures/aws_sdk_go_v2/` and pins the upstream commit in
  `manifest.json`.

  Exit codes: 0 = fixtures already up to date; 2 = fixtures were updated
  (CI drift signal); 1 = task failure. Requires OTP 25+ (verified TLS via
  `:public_key.cacerts_get/0`). Set `GITHUB_TOKEN` for API rate-limit headroom.

  Not shipped in the hex package.
  """
  use Mix.Task

  @repo "aws/aws-sdk-go-v2"
  @upstream_path "aws/protocol/eventstream/testdata"
  @corpus_dir "test/fixtures/aws_sdk_go_v2"

  # run/1 arrives in a later commit (fetch + wiring).

  @doc false
  def changeset(fetched, dir) do
    local = local_files(dir)
    fetched_paths = fetched |> Map.keys() |> MapSet.new()

    changed =
      for path <- MapSet.intersection(local, fetched_paths),
          File.read!(Path.join(dir, path)) != Map.fetch!(fetched, path),
          do: path

    %{
      added: MapSet.difference(fetched_paths, local) |> Enum.sort(),
      changed: Enum.sort(changed),
      removed: MapSet.difference(local, fetched_paths) |> Enum.sort()
    }
  end

  @doc false
  def apply_sync(fetched, changeset, commit_sha, dir) do
    for path <- changeset.added ++ changeset.changed do
      dest = Path.join(dir, path)
      File.mkdir_p!(Path.dirname(dest))
      File.write!(dest, Map.fetch!(fetched, path))
    end

    for path <- changeset.removed, do: File.rm!(Path.join(dir, path))

    File.write!(Path.join(dir, "manifest.json"), manifest_json(fetched, commit_sha))
    :ok
  end

  @doc false
  def manifest_json(fetched, commit_sha) do
    files =
      for {path, bin} <- Enum.sort(fetched) do
        {path, Base.encode16(:crypto.hash(:sha256, bin), case: :lower)}
      end

    source = [
      {"repo", @repo},
      {"path", @upstream_path},
      {"ref", "main"},
      {"commit", commit_sha}
    ]

    %Jason.OrderedObject{
      values: [
        {"source", %Jason.OrderedObject{values: source}},
        {"files", %Jason.OrderedObject{values: files}}
      ]
    }
    |> Jason.encode!(pretty: true)
    |> Kernel.<>("\n")
  end

  @doc false
  def local_manifest_commit(dir) do
    with {:ok, bin} <- File.read(Path.join(dir, "manifest.json")),
         {:ok, %{"source" => %{"commit" => sha}}} <- Jason.decode(bin) do
      sha
    else
      _ -> nil
    end
  end

  @doc false
  def summary(%{added: added, changed: changed, removed: removed}, old_sha, new_sha) do
    lines =
      Enum.map(added, &"  added:   #{&1}") ++
        Enum.map(changed, &"  changed: #{&1}") ++
        Enum.map(removed, &"  removed: #{&1}")

    header = "upstream fixtures changed (#{@repo} #{short(old_sha)} -> #{short(new_sha)}):"

    compare =
      if old_sha, do: ["compare: https://github.com/#{@repo}/compare/#{old_sha}...#{new_sha}"], else: []

    Enum.join([header | lines] ++ compare, "\n")
  end

  defp short(nil), do: "(none)"
  defp short(sha), do: String.slice(sha, 0, 12)

  defp local_files(dir) do
    if File.dir?(dir) do
      dir
      |> Path.join("**")
      |> Path.wildcard()
      |> Enum.filter(&File.regular?/1)
      |> Enum.map(&Path.relative_to(&1, dir))
      |> Enum.reject(&(&1 == "manifest.json"))
      |> MapSet.new()
    else
      MapSet.new()
    end
  end
end
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test test/mix/tasks/aws_event_stream.sync_fixtures_test.exs`
Expected: all pass (7 tests, 0 failures).

- [ ] **Step 5: Format and commit**

```bash
mix format
jj describe -m "feat: pure changeset/manifest core of the fixture sync task"
jj new
```

---

### Task 3: Sync-task shell — fetch over verified TLS, run/1, first real sync

Adds the network layer and `run/1`, then performs the first real sync and commits the downloaded corpus. Network code is not unit-tested (spec: no network in the test suite); verification is running the task for real.

**Files:**
- Modify: `lib/mix/tasks/aws_event_stream.sync_fixtures.ex`
- Create (by running the task): `test/fixtures/aws_sdk_go_v2/**` (18 corpus files + `manifest.json`)

**Interfaces:**
- Consumes: Task 2's `changeset/2`, `apply_sync/4`, `local_manifest_commit/1`, `summary/3`.
- Produces: `mix aws_event_stream.sync_fixtures` (exit 0 clean / 2 drift / 1 failure) and the committed corpus at `test/fixtures/aws_sdk_go_v2/` that Task 4's tests read.

- [ ] **Step 1: Implement fetch + run/1**

In `lib/mix/tasks/aws_event_stream.sync_fixtures.ex`, replace the `# run/1 arrives in a later commit (fetch + wiring).` comment with:

```elixir
@impl Mix.Task
def run(_argv) do
  {sha, fetched} = fetch()
  changeset = changeset(fetched, @corpus_dir)

  if changeset == %{added: [], changed: [], removed: []} do
    Mix.shell().info(
      "fixtures up to date with #{@repo}@#{String.slice(sha, 0, 12)} (no changes)"
    )
  else
    old_sha = local_manifest_commit(@corpus_dir)
    apply_sync(fetched, changeset, sha, @corpus_dir)
    Mix.shell().info(summary(changeset, old_sha, sha))
    exit({:shutdown, 2})
  end
end

@doc false
def fetch do
  {:ok, _} = Application.ensure_all_started(:inets)
  {:ok, _} = Application.ensure_all_started(:ssl)

  sha =
    api!("/repos/#{@repo}/commits/main")
    |> Map.fetch!("sha")

  paths = list_files(sha, @upstream_path)

  if paths == [] do
    Mix.raise("upstream corpus listing came back empty — #{@repo}:#{@upstream_path}@#{sha}")
  end

  fetched =
    for path <- paths, into: %{} do
      rel = Path.relative_to(path, @upstream_path)
      {rel, get!("https://raw.githubusercontent.com/#{@repo}/#{sha}/#{path}", [])}
    end

  {sha, fetched}
end

defp list_files(sha, path) do
  case api!("/repos/#{@repo}/contents/#{path}?ref=#{sha}") do
    entries when is_list(entries) ->
      Enum.flat_map(entries, fn
        %{"type" => "file", "path" => p} -> [p]
        %{"type" => "dir", "path" => p} -> list_files(sha, p)
        other -> Mix.raise("unexpected contents entry under #{path}: #{inspect(other)}")
      end)

    other ->
      Mix.raise("unexpected contents response for #{path}: #{inspect(other)}")
  end
end

defp api!(path) do
  auth =
    case System.get_env("GITHUB_TOKEN") do
      nil -> []
      token -> [{"authorization", "Bearer #{token}"}]
    end

  headers = [{"accept", "application/vnd.github+json"} | auth]

  ("https://api.github.com" <> path)
  |> get!(headers)
  |> Jason.decode!()
end

defp get!(url, headers) do
  headers = [{"user-agent", "aws_event_stream fixture sync (mix task)"} | headers]

  request =
    {String.to_charlist(url),
     Enum.map(headers, fn {k, v} -> {String.to_charlist(k), String.to_charlist(v)} end)}

  ssl_opts = [
    verify: :verify_peer,
    cacerts: :public_key.cacerts_get(),
    depth: 3,
    customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
  ]

  http_opts = [ssl: ssl_opts, timeout: 30_000, connect_timeout: 10_000]

  case :httpc.request(:get, request, http_opts, body_format: :binary) do
    {:ok, {{_http, 200, _status}, _resp_headers, body}} ->
      body

    {:ok, {{_http, code, status}, _resp_headers, body}} ->
      Mix.raise("GET #{url} -> #{code} #{status}: #{String.slice(body, 0, 200)}")

    {:error, reason} ->
      Mix.raise("GET #{url} failed: #{inspect(reason)}")
  end
end
```

- [ ] **Step 2: Compile clean and confirm the pure-core tests still pass**

Run: `mix test test/mix/tasks/aws_event_stream.sync_fixtures_test.exs`
Expected: pass, no warnings.

- [ ] **Step 3: First real sync (network)**

Run: `mix aws_event_stream.sync_fixtures; echo "exit: $?"`
Expected: summary listing 18 files as `added:` (9 under `encoded/`, 9 under `decoded/`), `(none) -> <12-hex-chars>` header, no compare link, then `exit: 2`.

Sanity-check the corpus:

Run: `find test/fixtures/aws_sdk_go_v2 -type f | sort && wc -c test/fixtures/aws_sdk_go_v2/encoded/positive/all_headers`
Expected: 19 files (18 corpus + manifest.json); `all_headers` is 204 bytes.

- [ ] **Step 4: Verify idempotence (acceptance criterion 1)**

Run: `mix aws_event_stream.sync_fixtures; echo "exit: $?"`
Expected: `fixtures up to date with aws/aws-sdk-go-v2@<sha12> (no changes)` and `exit: 0`.

- [ ] **Step 5: Verify tamper detection (acceptance criterion 3)**

Run: `printf 'x' >> test/fixtures/aws_sdk_go_v2/encoded/positive/empty_message && mix aws_event_stream.sync_fixtures; echo "exit: $?"`
Expected: summary with `changed: encoded/positive/empty_message`, a compare link, `exit: 2`, and the file restored to 16 bytes (`wc -c` it to confirm).

- [ ] **Step 6: Format and commit task + corpus**

```bash
mix format
jj describe -m "feat: mix aws_event_stream.sync_fixtures + import aws-sdk-go-v2 test corpus

Fetches aws/protocol/eventstream/testdata over verified TLS (:httpc,
verify_peer + public_key cacerts), mirrors it under test/fixtures/aws_sdk_go_v2
with a manifest pinning the upstream commit. Exit 2 signals drift to CI."
jj new
```

---

### Task 4: SDK vectors test — corpus walkers, JSON→Message builder, error map

**Files:**
- Modify: `test/support/fixtures.ex`
- Create: `test/aws_event_stream/sdk_vectors_test.exs`
- Test (builder unit tests): `test/aws_event_stream/fixtures_test.exs`

**Interfaces:**
- Consumes: the committed corpus from Task 3; `Decoder.decode/1`; `Encoder.encode/1`.
- Produces (in `AWSEventStream.Fixtures`):
  - `sdk_corpus_dir() :: Path.t()`
  - `sdk_cases(:positive | :negative) :: [%{name: String.t(), encoded: binary(), decoded: binary()}]` — `decoded` is the raw file body (JSON text for positive, prose for negative); raises naming the orphan if encoded/decoded case names differ.
  - `message_from_json(map()) :: Message.t()`
  - `expected_error_atom(String.t()) :: atom()` — raises on unmapped prose.
  - `unsigned32(integer()) :: non_neg_integer()`

- [ ] **Step 1: Write failing unit tests for the builder + error map**

Create `test/aws_event_stream/fixtures_test.exs`:

```elixir
defmodule AWSEventStream.FixturesTest do
  use ExUnit.Case, async: true
  alias AWSEventStream.{Fixtures, Header, Message}

  test "message_from_json maps every header type byte and base64 field" do
    json = %{
      "headers" => [
        %{"name" => "t", "type" => 0, "value" => true},
        %{"name" => "f", "type" => 1, "value" => false},
        %{"name" => "byte", "type" => 2, "value" => -49},
        %{"name" => "short", "type" => 3, "value" => 42},
        %{"name" => "int", "type" => 4, "value" => 40972},
        %{"name" => "long", "type" => 5, "value" => 42_424_242},
        %{"name" => "buf", "type" => 6, "value" => Base.encode64("raw")},
        %{"name" => "str", "type" => 7, "value" => Base.encode64("application/json")},
        %{"name" => "ts", "type" => 8, "value" => 8_675_309},
        %{"name" => "uuid", "type" => 9, "value" => Base.encode64(<<1::128>>)}
      ],
      "payload" => Base.encode64("{'foo':'bar'}")
    }

    assert %Message{headers: headers, payload: "{'foo':'bar'}"} = Fixtures.message_from_json(json)

    assert headers == [
             %Header{name: "t", type: :bool, value: true},
             %Header{name: "f", type: :bool, value: false},
             %Header{name: "byte", type: :byte, value: -49},
             %Header{name: "short", type: :short, value: 42},
             %Header{name: "int", type: :integer, value: 40972},
             %Header{name: "long", type: :long, value: 42_424_242},
             %Header{name: "buf", type: :bytes, value: "raw"},
             %Header{name: "str", type: :string, value: "application/json"},
             %Header{name: "ts", type: :timestamp, value: DateTime.from_unix!(8_675_309, :millisecond)},
             %Header{name: "uuid", type: :uuid, value: <<1::128>>}
           ]
  end

  test "message_from_json tolerates absent headers/payload (empty message)" do
    assert Fixtures.message_from_json(%{}) == %Message{headers: [], payload: ""}
  end

  test "expected_error_atom maps the known upstream descriptions" do
    assert Fixtures.expected_error_atom("Prelude checksum mismatch") == :invalid_prelude_crc
    assert Fixtures.expected_error_atom("Message checksum mismatch\n") == :invalid_message_crc
  end

  test "expected_error_atom raises loudly on unmapped prose" do
    assert_raise RuntimeError, ~r/unmapped upstream error description.*Frame too big/s, fn ->
      Fixtures.expected_error_atom("Frame too big")
    end
  end

  test "unsigned32 normalizes Go's signed int32 serialization" do
    assert Fixtures.unsigned32(-1_415_188_212) == 2_879_779_084
    assert Fixtures.unsigned32(263_087_306) == 263_087_306
  end
end
```

- [ ] **Step 2: Run to verify failure**

Run: `mix test test/aws_event_stream/fixtures_test.exs`
Expected: failures — `message_from_json/1` etc. undefined.

- [ ] **Step 3: Extend the Fixtures module**

In `test/support/fixtures.ex`, add below `bedrock_exception_frame/0` (inside the module):

```elixir
@sdk_dir Path.join(__DIR__, "../fixtures/aws_sdk_go_v2")

@doc "Root of the synced aws-sdk-go-v2 corpus (see mix aws_event_stream.sync_fixtures)."
def sdk_corpus_dir, do: @sdk_dir

@doc """
All corpus cases of one polarity, sorted by name. `decoded` is the raw file
body: JSON text for `:positive`, an expected-error description for `:negative`.
"""
def sdk_cases(kind) when kind in [:positive, :negative] do
  enc_dir = Path.join([@sdk_dir, "encoded", Atom.to_string(kind)])
  dec_dir = Path.join([@sdk_dir, "decoded", Atom.to_string(kind)])
  encoded = case_names(enc_dir)
  decoded = case_names(dec_dir)

  if encoded != decoded do
    raise "encoded/decoded #{kind} cases differ — orphans: " <>
            inspect((encoded -- decoded) ++ (decoded -- encoded))
  end

  for name <- encoded do
    %{
      name: name,
      encoded: File.read!(Path.join(enc_dir, name)),
      decoded: File.read!(Path.join(dec_dir, name))
    }
  end
end

defp case_names(dir) do
  case File.ls(dir) do
    {:ok, names} -> Enum.sort(names)
    {:error, _} -> []
  end
end

@doc "Build the expected %Message{} from an upstream decoded/positive JSON map."
def message_from_json(json) when is_map(json) do
  headers = json |> Map.get("headers") |> List.wrap() |> Enum.map(&header_from_json/1)

  payload =
    case Map.get(json, "payload") do
      nil -> ""
      b64 -> Base.decode64!(b64)
    end

  %Message{headers: headers, payload: payload}
end

defp header_from_json(%{"name" => name, "type" => type, "value" => value}) do
  {type_atom, decoded} = header_value(type, value)
  %Header{name: name, type: type_atom, value: decoded}
end

defp header_value(0, true), do: {:bool, true}
defp header_value(1, false), do: {:bool, false}
defp header_value(2, v), do: {:byte, v}
defp header_value(3, v), do: {:short, v}
defp header_value(4, v), do: {:integer, v}
defp header_value(5, v), do: {:long, v}
defp header_value(6, v), do: {:bytes, Base.decode64!(v)}
defp header_value(7, v), do: {:string, Base.decode64!(v)}
defp header_value(8, v), do: {:timestamp, DateTime.from_unix!(v, :millisecond)}
defp header_value(9, v), do: {:uuid, Base.decode64!(v)}

@error_map %{
  "Prelude checksum mismatch" => :invalid_prelude_crc,
  "Message checksum mismatch" => :invalid_message_crc
}

@doc "Map an upstream decoded/negative description to our decoder's error atom."
def expected_error_atom(prose) do
  trimmed = String.trim(prose)

  Map.get(@error_map, trimmed) ||
    raise "unmapped upstream error description: #{inspect(trimmed)} — " <>
            "upstream added a corruption mode; extend @error_map in #{__ENV__.file}"
end

@doc "Go serializes CRCs as signed int32; normalize to the wire's unsigned u32."
def unsigned32(n) when n < 0, do: n + 0x1_0000_0000
def unsigned32(n) when n >= 0, do: n
```

- [ ] **Step 4: Run builder tests to verify they pass**

Run: `mix test test/aws_event_stream/fixtures_test.exs`
Expected: 5 tests, 0 failures.

- [ ] **Step 5: Write the corpus test (goes green immediately — the corpus and decoder fix already landed)**

Create `test/aws_event_stream/sdk_vectors_test.exs`:

```elixir
defmodule AWSEventStream.SDKVectorsTest do
  @moduledoc """
  Interoperability against the corpus AWS itself tests with, synced from
  aws-sdk-go-v2 by `mix aws_event_stream.sync_fixtures`.
  """
  use ExUnit.Case, async: true
  alias AWSEventStream.{Decoder, Encoder, Fixtures}

  test "the SDK corpus is present and non-empty" do
    assert File.dir?(Fixtures.sdk_corpus_dir()),
           "missing #{Fixtures.sdk_corpus_dir()} — run `mix aws_event_stream.sync_fixtures`"

    refute Fixtures.sdk_cases(:positive) == [], "no positive cases in the corpus"
    refute Fixtures.sdk_cases(:negative) == [], "no negative cases in the corpus"
  end

  test "each positive vector decodes to the upstream-described message and re-encodes byte-identically" do
    for %{name: name, encoded: frame, decoded: json_bin} <- Fixtures.sdk_cases(:positive) do
      json = Jason.decode!(json_bin)
      expected = Fixtures.message_from_json(json)

      assert {[{:ok, ^expected}], <<>>} = Decoder.decode(frame), "decode mismatch: #{name}"
      assert IO.iodata_to_binary(Encoder.encode(expected)) == frame, "encode mismatch: #{name}"

      # the JSON's frame metadata agrees with the actual bytes
      assert byte_size(frame) == json["total_length"], "total_length mismatch: #{name}"
      <<_total::big-32, hlen::big-32, pcrc::big-32, _::binary>> = frame
      <<mcrc::big-32>> = binary_part(frame, byte_size(frame) - 4, 4)
      assert hlen == json["headers_length"], "headers_length mismatch: #{name}"
      assert pcrc == Fixtures.unsigned32(json["prelude_crc"]), "prelude_crc mismatch: #{name}"
      assert mcrc == Fixtures.unsigned32(json["message_crc"]), "message_crc mismatch: #{name}"
    end
  end

  test "each negative vector surfaces the upstream-described error" do
    for %{name: name, encoded: frame, decoded: prose} <- Fixtures.sdk_cases(:negative) do
      expected = Fixtures.expected_error_atom(prose)
      assert {[{:error, {^expected, _raw}}], _rest} = Decoder.decode(frame), "error mismatch: #{name}"
    end
  end
end
```

- [ ] **Step 6: Run the full suite**

Run: `mix test`
Expected: 0 failures; the three new SDK-vectors tests exercise 5 positive + 4 negative upstream cases.

- [ ] **Step 7: Format and commit**

```bash
mix format
jj describe -m "test: golden vectors from the aws-sdk-go-v2 corpus

Positive cases assert decode + byte-identical re-encode + CRC/length metadata;
negative cases assert the mapped error atom, and an unmapped upstream error
description fails loudly."
jj new
```

---

### Task 5: Keep the sync task out of the hex package

**Files:**
- Modify: `mix.exs` (the `package/0` `files:` entry)

**Interfaces:**
- Consumes / Produces: nothing code-level; changes only what `mix hex.build` bundles.

- [ ] **Step 1: Narrow the package files**

In `mix.exs`, replace:

```elixir
files: ~w(lib mix.exs README.md LICENSE)
```

with:

```elixir
# lib/mix/ holds maintainer-only tasks — not shipped to hex consumers.
files: ~w(lib/aws_event_stream lib/aws_event_stream.ex mix.exs README.md LICENSE)
```

- [ ] **Step 2: Verify the tarball contents (acceptance criterion 5)**

Run: `mix hex.build | grep -A30 "Files:"`
Expected: every `lib/aws_event_stream/**` file listed; **no** `lib/mix/` entry.

- [ ] **Step 3: Commit**

```bash
mix format
jj describe -m "build: exclude lib/mix maintainer tasks from the hex package"
jj new
```

---

### Task 6: Watcher workflow

**Files:**
- Create: `.github/workflows/fixtures-watch.yml`

**Interfaces:**
- Consumes: `mix aws_event_stream.sync_fixtures` exit-code contract (0 clean / 2 drift / else crash) and its stdout summary.
- Produces: a scheduled workflow that opens or comments on an issue labeled `upstream-fixtures-drift`.

- [ ] **Step 1: Write the workflow**

Create `.github/workflows/fixtures-watch.yml`:

```yaml
name: Fixtures Watch

on:
  schedule:
    - cron: "0 6 * * 1" # Mondays 06:00 UTC
  workflow_dispatch:

permissions:
  contents: read
  issues: write

jobs:
  watch:
    runs-on: ubuntu-latest
    env:
      GH_TOKEN: ${{ github.token }}
      GITHUB_TOKEN: ${{ github.token }}
    steps:
      - uses: actions/checkout@v4
      - uses: erlef/setup-beam@v1
        with:
          otp-version: "27"
          elixir-version: "1.17"
      - run: mix deps.get

      - name: Sync fixtures from upstream
        id: sync
        run: |
          set +e
          mix aws_event_stream.sync_fixtures | tee sync_summary.txt
          code=${PIPESTATUS[0]}
          echo "exit_code=$code" >> "$GITHUB_OUTPUT"
          # 0 = up to date, 2 = drift; anything else is a task failure
          if [ "$code" != "0" ] && [ "$code" != "2" ]; then
            echo "sync task failed unexpectedly (exit $code)"
            exit "$code"
          fi

      - name: Test codec against the synced fixtures
        id: test
        if: steps.sync.outputs.exit_code == '2'
        run: |
          set +e
          mix test 2>&1 | tee test_output.txt
          echo "exit_code=${PIPESTATUS[0]}" >> "$GITHUB_OUTPUT"
          exit 0

      - name: Open or update the drift issue
        if: steps.sync.outputs.exit_code == '2'
        run: |
          if [ "${{ steps.test.outputs.exit_code }}" = "0" ]; then
            headline="**mix test PASSES against the new vectors** — refreshing the fixtures is routine."
          else
            headline="**mix test FAILS against the new vectors** — a codec change may be needed (see workflow logs)."
          fi
          {
            echo "The upstream aws-sdk-go-v2 event-stream test corpus changed."
            echo
            echo "$headline"
            echo
            echo '```'
            cat sync_summary.txt
            echo '```'
            echo
            echo 'To refresh: run `mix aws_event_stream.sync_fixtures` locally, review, and commit.'
            echo
            echo "_Reported by the scheduled fixtures-watch workflow._"
          } > issue_body.md
          gh label create upstream-fixtures-drift --force \
            --description "Upstream AWS SDK test vectors drifted" --color D93F0B
          existing=$(gh issue list --label upstream-fixtures-drift --state open \
            --json number --jq '.[0].number')
          if [ -n "$existing" ]; then
            gh issue comment "$existing" --body-file issue_body.md
          else
            gh issue create --title "Upstream event-stream test vectors changed" \
              --label upstream-fixtures-drift --body-file issue_body.md
          fi
```

- [ ] **Step 2: Validate the YAML parses**

Run: `mix run -e 'IO.inspect(:yamerl_constr.string(File.read!(".github/workflows/fixtures-watch.yml") |> String.to_charlist()) |> length())' 2>/dev/null || ruby -ryaml -e 'YAML.load_file(".github/workflows/fixtures-watch.yml"); puts "yaml ok"'`
Expected: `yaml ok` (macOS ships ruby; yamerl isn't a dep). If neither is available, `python3 -c 'import yaml,sys; yaml.safe_load(open(".github/workflows/fixtures-watch.yml")); print("yaml ok")'`.

- [ ] **Step 3: Commit**

```bash
jj describe -m "ci: weekly watcher syncs upstream fixtures and files a drift issue"
jj new
```

- [ ] **Step 4 (post-push, manual): trigger once via workflow_dispatch**

After the branch lands on GitHub: `gh workflow run "Fixtures Watch" && gh run watch`
Expected: green run, sync step logs "fixtures up to date", no issue created. (This step can only happen after push — note it in the final handoff, don't block on it.)

---

### Task 7: Docs — README + fixtures.ex comment

**Files:**
- Modify: `README.md` (Status / Future work sections)
- Modify: `test/support/fixtures.ex` (the stale MIM-43 comment)

**Interfaces:** none — prose only.

- [ ] **Step 1: Update the stale comment in `test/support/fixtures.ex`**

Replace the comment block above `golden_vectors/0` (the lines starting "# Self-encoded regression locks..." through "...tracked in MIM-43.") with:

```elixir
# Self-encoded regression locks that pin the wire format of known messages.
# The hex bytes were generated by running AWSEventStream.Encoder.encode/1 against
# the corresponding Message struct and freezing the output. They are NOT transcribed
# from an external source — they lock in the codec's own wire format.
#
# Externally-sourced vectors live in test/fixtures/aws_sdk_go_v2/ (synced from
# aws-sdk-go-v2 by `mix aws_event_stream.sync_fixtures`, exercised by
# SDKVectorsTest).
```

- [ ] **Step 2: Update the README**

In the **Status / Non-goals** section:

1. Run `mix test` and note the test count from the `N tests, 0 failures` line. Replace the status sentence's `26 tests covering ...` with the new count and append `, and the aws-sdk-go-v2 golden-vector corpus` to the coverage list.
2. Remove the future-work bullet `- Externally-sourced golden vectors (upstream SDK test corpora).`
3. Keep the "Stream watcher / live capture tooling" non-goal — the fixtures watcher watches the *SDK corpus*, not live streams; no change needed there.

After the **Status / Non-goals** section, add:

```markdown
## Upstream test vectors

`test/fixtures/aws_sdk_go_v2/` mirrors the event-stream test corpus from
[`aws/aws-sdk-go-v2`](https://github.com/aws/aws-sdk-go-v2/tree/main/aws/protocol/eventstream/testdata)
byte-for-byte; `manifest.json` pins the upstream commit. The suite decodes
every positive vector to the upstream-described message (and re-encodes it
byte-identically), and asserts each corrupted vector surfaces the documented
error.

Maintainers refresh the corpus with:

```sh
mix aws_event_stream.sync_fixtures   # exit 0 = up to date, 2 = fixtures updated
```

A weekly GitHub Actions workflow (`fixtures-watch.yml`) runs the same task and
opens an issue when the upstream corpus drifts, stating whether the codec still
passes against the new vectors.
```

- [ ] **Step 3: Full verification pass**

Run: `mix format --check-formatted && mix test`
Expected: format clean, 0 failures.

- [ ] **Step 4: Commit**

```bash
jj describe -m "docs: document the upstream vector corpus and sync task"
jj new
```

---

## Final verification (whole-plan acceptance)

- [ ] `mix aws_event_stream.sync_fixtures` exits 0 ("no changes") on the finished branch.
- [ ] `mix test` — 0 failures.
- [ ] `mix format --check-formatted` — clean.
- [ ] `mix hex.build` — no `lib/mix/` in the file list.
- [ ] `jj log --git` shows one commit per task, each self-contained.
- [ ] Hand off with superpowers:finishing-a-development-branch (push + note the post-push `workflow_dispatch` smoke test from Task 6 Step 4).
