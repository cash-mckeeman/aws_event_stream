defmodule AWSEventStream.Decoder do
  @moduledoc """
  Incremental decoder for `vnd.amazon.eventstream` frames.

  `decode/2` consumes as many whole frames as the buffer holds and returns
  `{results, rest}`, where `rest` is leftover bytes to prepend to the next
  chunk. Frame-decode errors are surfaced as `{:error, {reason, raw_frame}}`
  results (never silently dropped); pass `on_error: :skip` to drop them.
  """
  alias AWSEventStream.{Header, Message}

  @prelude_size 12
  @crc_size 4
  @min_size @prelude_size + @crc_size

  @type result :: {:ok, Message.t()} | {:error, {atom(), binary()}}

  @spec decode(binary(), keyword()) :: {[result()], binary()}
  def decode(buffer, opts \\ []) when is_binary(buffer) do
    on_error = Keyword.get(opts, :on_error, :keep)
    {results, rest} = decode_loop(buffer, [])

    results =
      if on_error == :skip, do: Enum.reject(results, &match?({:error, _}, &1)), else: results

    {results, rest}
  end

  defp decode_loop(buffer, acc) when byte_size(buffer) < @prelude_size do
    {Enum.reverse(acc), buffer}
  end

  defp decode_loop(<<total::big-32, hlen::big-32, pcrc::big-32, rest::binary>> = buffer, acc) do
    cond do
      total < @min_size ->
        {Enum.reverse([{:error, {:invalid_message_length, buffer}} | acc]), <<>>}

      byte_size(buffer) < total ->
        # whole frame not yet available — hand the buffer back for the next chunk
        {Enum.reverse(acc), buffer}

      true ->
        body_len = total - @prelude_size - hlen - @crc_size

        <<headers_bin::binary-size(^hlen), payload::binary-size(^body_len), mcrc::big-32,
          tail::binary>> = rest

        frame = binary_part(buffer, 0, total)
        result = verify(total, hlen, pcrc, headers_bin, payload, mcrc, frame)
        decode_loop(tail, [result | acc])
    end
  end

  defp verify(total, hlen, pcrc, headers_bin, payload, mcrc, frame) do
    prelude = <<total::big-32, hlen::big-32>>

    cond do
      :erlang.crc32(prelude) != pcrc ->
        {:error, {:invalid_prelude_crc, frame}}

      :erlang.crc32(binary_part(frame, 0, total - @crc_size)) != mcrc ->
        {:error, {:invalid_message_crc, frame}}

      true ->
        {:ok, %Message{headers: Header.decode_all(headers_bin), payload: payload}}
    end
  end
end
