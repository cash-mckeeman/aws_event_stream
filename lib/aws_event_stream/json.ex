defmodule AWSEventStream.JSON do
  @moduledoc """
  Optional Bedrock/JSON convenience layer over the core codec.

  Classifies each frame by its `:message-type` header (`event` / `exception` /
  `error`) — **before** `:event-type` — into idiomatic tagged tuples, and
  unwraps Bedrock JSON payloads (`{"bytes": base64}` or direct JSON). Requires
  the optional `:jason` dependency.
  """
  alias AWSEventStream.{Decoder, Message}

  @type classified ::
          {:event, String.t() | nil, map()}
          | {:exception, String.t() | nil, map()}
          | {:error, String.t() | nil, String.t() | nil}
          | {:malformed_payload, Message.t(), term()}

  @spec decode(binary(), keyword()) ::
          {[classified() | {:malformed_frame, atom(), binary()}], binary()}
  def decode(buffer, opts \\ []) when is_binary(buffer) do
    {results, rest} = Decoder.decode(buffer, opts)

    classified =
      Enum.map(results, fn
        {:ok, msg} -> classify(msg)
        {:error, {reason, raw}} -> {:malformed_frame, reason, raw}
      end)

    {classified, rest}
  end

  @spec classify(Message.t()) :: classified()
  def classify(%Message{} = msg) do
    case Message.header(msg, ":message-type") do
      "exception" ->
        with {:ok, payload} <- decode_payload(msg) do
          {:exception, Message.header(msg, ":exception-type"), payload}
        end

      "error" ->
        {:error, Message.header(msg, ":error-code"), Message.header(msg, ":error-message")}

      _event_or_nil ->
        with {:ok, payload} <- decode_payload(msg) do
          {:event, Message.header(msg, ":event-type"), payload}
        end
    end
  end

  # Returns {:ok, map} or a {:malformed_payload, msg, reason} that propagates out of `with`.
  defp decode_payload(%Message{payload: payload} = msg) do
    case Jason.decode(payload) do
      {:ok, %{"bytes" => b64}} ->
        case Base.decode64(b64) do
          {:ok, raw} ->
            case Jason.decode(raw) do
              {:ok, map} -> {:ok, map}
              {:error, reason} -> {:malformed_payload, msg, reason}
            end

          :error ->
            {:malformed_payload, msg, :invalid_base64}
        end

      {:ok, map} when is_map(map) ->
        {:ok, map}

      {:ok, _non_object} ->
        # Valid JSON but not an object (list/number/string) — payload must be a map.
        {:malformed_payload, msg, :not_an_object}

      {:error, reason} ->
        {:malformed_payload, msg, reason}
    end
  end
end
