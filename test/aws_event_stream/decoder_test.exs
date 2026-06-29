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
    {whole, partial} = String.split_at(bin, byte_size(bin) - 3)
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

  test "negative body_len from a malformed headers_len yields an error, not a crash" do
    # total=20 (>=16, passes the length guard); hlen=10 -> body_len = 20-12-10-4 = -6
    # buffer carries exactly `total` bytes so we reach the body split branch.
    buffer = <<20::big-32, 10::big-32, 0::big-32>> <> :binary.copy(<<0>>, 8)
    assert {[{:error, {:invalid_message_length, ^buffer}}], <<>>} = Decoder.decode(buffer)
  end

  test "a too-small total_len yields :invalid_message_length" do
    buffer = <<10::big-32, 0::big-32, 0::big-32>>
    assert {[{:error, {:invalid_message_length, _}}], <<>>} = Decoder.decode(buffer)
  end

  test "a corrupt prelude CRC yields :invalid_prelude_crc" do
    good = IO.iodata_to_binary(Encoder.encode(%Message{headers: [], payload: "hello"}))
    <<pre::binary-8, _pcrc::big-32, tail::binary>> = good
    corrupt = <<pre::binary, 0::big-32, tail::binary>>
    assert {[{:error, {:invalid_prelude_crc, ^corrupt}}], <<>>} = Decoder.decode(corrupt)
  end
end
