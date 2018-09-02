defmodule CodeshipDB.Conn do
  alias CodeshipDB.Pkt

  defstruct message: nil,
            decoded_message: nil,
            client_ip: nil,
            client_port: nil,
            response: nil

  def realm(), do: "CodeShip.com"

  def respond(status, pkt, code, data, key \\ nil)

  def respond(:ok, pkt, _code, data, key),
    do: do_respond(%{data: data}, :response, pkt.method, pkt.transactionid, pkt.integrity, key)

  def respond(_, pkt, code, data, key),
    do:
      do_respond(
        %{error_code: {code, data}},
        :error,
        pkt.method,
        pkt.transactionid,
        pkt.integrity,
        key
      )

  def do_respond(attrs, class, method, transid, integrity, key),
    do:
      %Pkt{
        attrs: Map.put(attrs, :realm, realm()),
        class: class,
        method: method,
        transactionid: transid,
        integrity: integrity
      }
      |> Pkt.encode(key)
end
