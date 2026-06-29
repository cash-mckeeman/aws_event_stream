defmodule AWSEventStream.Fixtures do
  @moduledoc false
  @dir Path.join(__DIR__, "../fixtures")

  def golden_vectors do
    @dir |> Path.join("golden_vectors.exs") |> Code.eval_file() |> elem(0)
  end

  def bedrock_exception_frame do
    @dir |> Path.join("bedrock_exception.bin") |> File.read!()
  end
end
