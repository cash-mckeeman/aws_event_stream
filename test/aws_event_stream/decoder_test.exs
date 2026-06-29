defmodule AWSEventStream.DecoderTest do
  use ExUnit.Case, async: true
  alias AWSEventStream.{Decoder, Encoder, Header, Message}

  defp frame(msg), do: IO.iodata_to_binary(Encoder.encode(msg))

  test "decodes a single encoded frame" do
    msg = %Message{
      headers: [%Header{name: ":event-type", type: :string, value: "chunk"}],
      payload: "{}"
    }

    assert {[{:ok, ^msg}], <<>>} = Decoder.decode(frame(msg))
  end

  test "decodes multiple frames and returns trailing partial as rest" do
    a = %Message{headers: [], payload: "a"}
    b = %Message{headers: [], payload: "b"}
    bin = frame(a) <> frame(b)
    {whole, partial} = String.split_at(bin, byte_size(bin) - 3) |> then(fn {w, p} -> {w, p} end)
    {results, rest} = Decoder.decode(whole <> partial)
    assert results == [ok: a, ok: b]
    assert rest == <<>>

    # truncate b mid-frame: a decodes, b's bytes come back as rest
    truncated = frame(a) <> binary_part(frame(b), 0, byte_size(frame(b)) - 2)
    assert {[{:ok, ^a}], leftover} = Decoder.decode(truncated)
    assert leftover == binary_part(frame(b), 0, byte_size(frame(b)) - 2)
  end

  test "surfaces a message-CRC error by default, skips with :on_error :skip" do
    msg = %Message{headers: [], payload: "hello"}
    good = frame(msg)
    # clobber last CRC byte
    corrupt = binary_part(good, 0, byte_size(good) - 1) <> <<0>>
    assert {[{:error, {:invalid_message_crc, ^corrupt}}], <<>>} = Decoder.decode(corrupt)
    assert {[], <<>>} = Decoder.decode(corrupt, on_error: :skip)
  end
end
