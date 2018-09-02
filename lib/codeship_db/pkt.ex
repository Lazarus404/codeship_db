defmodule CodeshipDB.Pkt do
  use Bitwise
  require Logger
  alias CodeshipDB.Pkt.Params

  @pkt_magic_cookie "SHIP"
  @infix 3

  defstruct class: nil,
            method: nil,
            transactionid: nil,
            integrity: false,
            key: nil,
            attrs: %{}

  def decode(pkt_binary, key \\ nil) do
    {integrity, pkt_binary} = check_integrity(pkt_binary, key)

    <<@pkt_magic_cookie, @infix::2, m0::5, c0::1, m1::3, c1::1, m2::4, length::16,
      transactionid::96, rest::binary>> = pkt_binary

    method = get_method(<<m0::5, m1::3, m2::4>>)
    class = get_class(<<c0::1, c1::1>>)
    attrs = decode_attrs(rest, length, transactionid)

    {:ok,
     %__MODULE__{
       class: class,
       method: method,
       integrity: integrity,
       key: key,
       transactionid: transactionid,
       attrs: attrs
     }}
  end

  def encode(%__MODULE__{} = config, nkey \\ nil) do
    m = get_method_id(config.method)
    <<m0::5, m1::3, m2::4>> = <<m::12>>
    <<c0::1, c1::1>> = get_class_id(config.class)

    bin_attrs =
      for {t, v} <- config.attrs,
          into: "",
          do: encode_bin(encode_attribute(t, v, config.transactionid))

    length = byte_size(bin_attrs)

    pkt_binary_0 =
      <<@pkt_magic_cookie, @infix::2, m0::5, c0::1, m1::3, c1::1, m2::4, length::16,
        config.transactionid::96, bin_attrs::binary>>

    case config.integrity do
      false -> pkt_binary_0
      true -> insert_integrity(pkt_binary_0, nkey)
    end
  end

  # -------------------------------------------------------------------------------
  # Start code generation
  # -------------------------------------------------------------------------------

  for {{name, type}, byte} <- Params.attrs() |> Enum.with_index() do
    case type do
      :value ->
        defp decode_attribute(unquote(byte), value, _),
          do: {unquote(name), value}

        defp encode_attribute(unquote(name), value, _),
          do: {unquote(byte), value}

      :error_attribute ->
        defp decode_attribute(unquote(byte), value, _tid),
          do: {unquote(name), decode_attr_err(value)}

        defp encode_attribute(unquote(name), value, _),
          do: {unquote(byte), encode_attr_err(value)}
    end
  end

  defp decode_attribute(byte, value, _) do
    Logger.error("Could not find match for #{inspect(byte)}")
    {byte, value}
  end

  defp encode_attribute(other, value, _) do
    Logger.error("Could not find match for #{inspect(other)}")
    {other, value}
  end

  for {name, id} <- Params.methods() |> Enum.with_index() do
    defp get_method(<<unquote(id)::size(12)>>),
      do: unquote(name)

    defp get_method_id(unquote(name)),
      do: unquote(id)
  end

  defp get_method(<<o::size(12)>>),
    do: o

  defp get_method_id(o),
    do: o

  for {name, id} <- Params.classes() |> Enum.with_index() do
    defp get_class(<<unquote(id)::size(2)>>),
      do: unquote(name)

    defp get_class_id(unquote(name)),
      do: <<unquote(id)::2>>
  end

  # -------------------------------------------------------------------------------
  # End code generation
  # -------------------------------------------------------------------------------

  # Converts a given binary encoded list of attributes into an Erlang list of tuples
  defp decode_attrs(pkt, len, tid, attrs \\ %{})

  defp decode_attrs(<<>>, _len, _, attrs), do: attrs

  defp decode_attrs(<<type::size(16), item_length::size(16), bin::binary>>, len, tid, attrs) do
    whole_pkt? = item_length == byte_size(bin)

    padding_length =
      case rem(item_length, 4) do
        0 -> 0
        _ when whole_pkt? -> 0
        other -> 4 - other
      end

    <<value::binary-size(item_length), _::binary-size(padding_length), rest::binary>> = bin
    {t, v} = decode_attribute(type, value, tid)
    new_length = len - (2 + 2 + item_length + padding_length)
    decode_attrs(rest, new_length, tid, Map.put(attrs, t, v))
  end

  # Converts a given binary encoded error into an Erlang tuple
  defp decode_attr_err(<<_mbz::size(20), class::size(4), number::size(8), reason::binary>>),
    do: {class * 100 + number, reason}

  #####
  # Encoding helpers

  # Encodes an attribute tuple into its specific encoded binary
  defp encode_bin({_, nil}), do: <<>>

  defp encode_bin({t, v}) do
    l = byte_size(v)

    padding_length =
      case rem(l, 4) do
        0 -> 0
        other -> (4 - other) * 8
      end

    <<t::16, l::16, v::binary-size(l), 0::size(padding_length)>>
  end

  # Encodes a error tuple into its binary representation
  defp encode_attr_err({error_code, reason}) do
    class = div(error_code, 100)
    number = rem(error_code, 100)
    <<0::size(20), class::size(4), number::size(8), reason::binary>>
  end

  #####
  # auth

  # full check of integrity
  defp check_integrity(pkt_binary, nil), do: {false, pkt_binary}

  defp check_integrity(pkt_binary, key) when byte_size(pkt_binary) > 20 + 24 do
    with s <- byte_size(pkt_binary) - 24,
         <<message::binary-size(s), 0x00::size(8), 0x08::size(8), 0x00::size(8), 0x14::size(8),
           integrity::binary-size(20)>> <- pkt_binary,
         ^integrity <- hmac_sha1(message, key) do
      <<h::size(16), old_size::size(16), payload::binary>> = message
      new_size = old_size - 24
      {true, <<h::size(16), new_size::size(16), payload::binary>>}
    else
      _ ->
        Logger.info("No MESSAGE-INTEGRITY was found in message.")
        {false, pkt_binary}
    end
  end

  # Inserts a valid integrity marker and value to the end of a binary
  defp insert_integrity(pkt_binary, nil),
    do: pkt_binary

  defp insert_integrity(pkt_binary, key) do
    Logger.info("INSERTING INTEGRITY WITH KEY #{inspect(key)}")
    <<0::2, type::14, len::16, magic::32, trid::96, attrs::binary>> = pkt_binary
    nlen = len + 4 + 20
    value = <<0::2, type::14, nlen::16, magic::32, trid::96, attrs::binary>>
    integrity = hmac_sha1(value, key)

    <<0::2, type::14, nlen::16, magic::32, trid::96, attrs::binary, 0x00::size(8), 0x08::size(8),
      0x00::size(8), 0x14::size(8), integrity::binary-size(20)>>
  end

  defp hmac_sha1(msg, hash) when is_binary(msg) and is_binary(hash) do
    key = :crypto.hash(:md5, to_charlist(hash))
    :crypto.hmac(:sha, key, msg)
  end
end
