defmodule AWSEventStream.Header do
  @moduledoc """
  A single AWS event-stream header value and its wire codec.

  Wire triple: `name_len:u8` + name + `value_type:u8` + value. All ~10 AWS
  value types are supported. Numeric values are signed big-endian; timestamps
  are `DateTime` at millisecond precision; uuids are raw 16-byte binaries.
  """

  @type value_type ::
          :bool | :byte | :short | :integer | :long | :bytes | :string | :timestamp | :uuid
  @type t :: %__MODULE__{name: String.t(), type: value_type(), value: term()}
  defstruct [:name, :type, :value]

  @doc "Decode a concatenated headers blob into a list of headers in wire order."
  @spec decode_all(binary()) :: [t()]
  def decode_all(bin) when is_binary(bin), do: decode_all(bin, [])

  defp decode_all(<<>>, acc), do: Enum.reverse(acc)

  defp decode_all(<<name_len::8, name::binary-size(name_len), type::8, rest::binary>>, acc) do
    {value, rest} = decode_value(type, rest)
    decode_all(rest, [%__MODULE__{name: name, type: type_name(type), value: value} | acc])
  end

  defp decode_value(0, rest), do: {true, rest}
  defp decode_value(1, rest), do: {false, rest}
  defp decode_value(2, <<v::signed-8, rest::binary>>), do: {v, rest}
  defp decode_value(3, <<v::signed-big-16, rest::binary>>), do: {v, rest}
  defp decode_value(4, <<v::signed-big-32, rest::binary>>), do: {v, rest}
  defp decode_value(5, <<v::signed-big-64, rest::binary>>), do: {v, rest}
  defp decode_value(6, <<len::big-16, v::binary-size(len), rest::binary>>), do: {v, rest}
  defp decode_value(7, <<len::big-16, v::binary-size(len), rest::binary>>), do: {v, rest}

  defp decode_value(8, <<ms::signed-big-64, rest::binary>>),
    do: {DateTime.from_unix!(ms, :millisecond), rest}

  defp decode_value(9, <<v::binary-16, rest::binary>>), do: {v, rest}

  defp type_name(0), do: :bool
  defp type_name(1), do: :bool
  defp type_name(2), do: :byte
  defp type_name(3), do: :short
  defp type_name(4), do: :integer
  defp type_name(5), do: :long
  defp type_name(6), do: :bytes
  defp type_name(7), do: :string
  defp type_name(8), do: :timestamp
  defp type_name(9), do: :uuid
end
