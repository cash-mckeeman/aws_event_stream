defmodule AWSEventStream.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/cash-mckeeman/aws_event_stream"

  def project do
    [
      app: :aws_event_stream,
      version: @version,
      elixir: ">= 1.15.0",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description:
        "Pure-Elixir codec for the AWS vnd.amazon.eventstream binary protocol (encode + decode).",
      package: package(),
      docs: docs(),
      source_url: @source_url,
      name: "AWSEventStream"
    ]
  end

  def application, do: [extra_applications: [:logger]]

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:jason, "~> 1.4", optional: true},
      {:stream_data, "~> 1.0", only: :test},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE)
    ]
  end

  defp docs, do: [main: "AWSEventStream", source_ref: "v#{@version}", source_url: @source_url]
end
