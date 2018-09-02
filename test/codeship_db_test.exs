defmodule CodeshipDBTest do
  use ExUnit.Case
  doctest CodeshipDB

  alias CodeshipDB.Store.{Client, Manager}
  alias CodeshipDB.{Pkt, Conn}

  describe "when working with direct calls" do
    test "set a simple string" do
      get_and_set("bar")
    end

    test "set a simple integer" do
      get_and_set(12345)
    end

    test "set a simple boolean" do
      get_and_set(true)
    end

    test "set a map" do
      get_and_set(%{"left" => "bar", "right" => "baz"})
    end

    test "update an existing collection" do
      bucket = "0"
      key = "data"
      Client.delete(bucket, key)

      selector1 = %{"key1" => "one", "key2" => "two"}
      selector2 = %{"key1" => "three", "key2" => "four"}

      delta1 = %{
        "$setOnInsert" => selector1,
        "$inc" => %{"integer" => 4, "float" => 2.2}
      }

      delta2 = %{
        "$setOnInsert" => selector2,
        "$inc" => %{"integer" => 5, "float" => 3.1}
      }

      assert :ok == Client.update(bucket, key, selector1, delta1)

      {:ok, result} = Client.get(bucket, key)
      assert result == [Map.merge(selector1, delta1["$inc"])]

      assert :ok == Client.update(bucket, key, selector1, delta1)

      {:ok, result} = Client.get(bucket, key)
      assert result == [Map.merge(selector1, %{"integer" => 8, "float" => 4.4})]

      assert :ok == Client.update(bucket, key, selector1, delta2)
      assert :ok == Client.update(bucket, key, selector2, delta2)

      {:ok, result} = Client.get(bucket, key)
      assert result |> Enum.member?(Map.merge(selector1, %{"integer" => 13, "float" => 7.5}))
      assert result |> Enum.member?(Map.merge(selector2, %{"integer" => 5, "float" => 3.1}))
    end
  end

  describe "when sending encoded packets" do
    test "received packet decodes to match sent data" do
      pkt = %Pkt{
        class: :request,
        method: :set,
        transactionid: 1,
        attrs: %{
          json: "{\"key\":\"my_key\",\"data\":\"abcde\"}"
        }
      }

      payload = Pkt.encode(pkt)

      %Conn{decoded_message: request} = Manager.process_pkt(payload)
      assert request == pkt
    end

    test "setting a value errors if not authenticated" do
      key = "my_key"
      value = "abcde"

      pkt = %Pkt{
        class: :request,
        method: :set,
        transactionid: 1,
        attrs: %{
          json: "{\"key\":\"#{key}\",\"data\":\"#{value}\"}"
        }
      }

      encoded = Manager.execute(%Conn{message: Pkt.encode(pkt), decoded_message: pkt}, "0")
      res = Manager.process_pkt(encoded)

      assert %CodeshipDB.Conn{
               decoded_message: %CodeshipDB.Pkt{
                 attrs: %{
                   error_code: {401, "Unauthorized"},
                   realm: "CodeShip.com"
                 },
                 class: :error,
                 integrity: false,
                 key: nil,
                 method: :set,
                 transactionid: 1
               }
             } = res
    end

    test "setting a value succeeds if authenticated" do
      key = "my_key"
      value = "abcde"

      pkt = %Pkt{
        class: :request,
        method: :set,
        transactionid: 1,
        attrs: %{
          json: "{\"key\":\"#{key}\",\"data\":\"#{value}\"}",
          username: "user",
          password: "pass"
        }
      }

      encoded = Manager.execute(%Conn{message: Pkt.encode(pkt), decoded_message: pkt}, "0")
      res = Manager.process_pkt(encoded)

      assert %CodeshipDB.Conn{
               client_ip: nil,
               client_port: nil,
               decoded_message: %CodeshipDB.Pkt{
                 attrs: %{realm: "CodeShip.com"},
                 class: :response,
                 integrity: false,
                 key: nil,
                 method: :set,
                 transactionid: 1
               }
             } = res

      pkt = %Pkt{
        class: :request,
        method: :get,
        transactionid: 1,
        attrs: %{
          json: "{\"key\":\"my_key\"}",
          username: "user",
          password: "pass"
        }
      }

      assert {:ok, ^value} =
               %Conn{message: Pkt.encode(pkt), decoded_message: pkt}
               |> Manager.execute("0")
               |> Manager.process_pkt()
               |> get_data()
    end
  end

  defp get_and_set(value) do
    result = Client.set("0", "foo", value)
    assert result == :ok
    {:ok, result} = Client.get("0", "foo")
    assert result == value
  end

  defp get_data(%Conn{decoded_message: %Pkt{attrs: %{data: value}}}), do: {:ok, value}
  defp get_data(_), do: {:error, :not_found}
end
