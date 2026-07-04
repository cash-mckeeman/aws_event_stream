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
      if old_sha,
        do: ["compare: https://github.com/#{@repo}/compare/#{old_sha}...#{new_sha}"],
        else: []

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
