defmodule CodeshipDB.Store.Manager do
  @moduledoc """
  handles the outer db, such as db selection and packet process handling.
  """
  use GenServer
  alias CodeshipDB.Store.{Client, Manager, Super}
  alias CodeshipDB.{Pkt, Conn}

  @password "pass"

  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Acquire packet from binary stream here, since
  we don't want to pollute container apps with knowledge of
  packet functions.
  """
  def process_pkt(pkt) do
    with {:ok, data} <- Pkt.decode(pkt) do
      %Conn{message: pkt, decoded_message: data}
    end
  end

  @doc """
  attempt to execute the command depicted by the packet structure
  """
  def execute(%Conn{decoded_message: data}, db \\ nil) do
    with true <- Map.has_key?(data.attrs, :username),
         {{:ok, key}, :auth} <- {process_integrity(Map.get(data.attrs, :username)), :auth},
         {:ok, key, :exec} <-
           {do_execute(data.method, data.attrs.json, Map.get(data.attrs, :bucket, db)), key,
            :exec} do
      Conn.respond(:ok, data, 200, nil, key)
    else
      false -> Conn.respond(:error, data, 401, "Unauthorized")
      {_, :auth} -> Conn.respond(:error, data, 401, "Unauthorized")
      {{status, value}, key, :exec} -> Conn.respond(status, data, 200, value, key)
    end
  end

  def do_execute(method, json, client) when is_binary(json) do
    with {:ok, data} <- Poison.decode(json),
         params when is_list(params) <- process_json(data) do
      GenServer.call(Manager, {method, params, client})
    end
  end

  def do_execute(method, data, client) do
    GenServer.call(Manager, {method, data, client})
  end

  #########################################################################################################################
  # OTP functions
  #########################################################################################################################

  def init([]) do
    {:ok, default} = Client.create()
    {:ok, %{"0" => default}}
  end

  def handle_call({:bucket_exists, _, client}, _from, state) do
    {:reply, Map.has_key?(state, client), state}
  end

  def handle_call({:destroy, _, client}, _from, state) do
    case Map.has_key?(state, client) do
      true ->
        state
        |> Map.get(client)
        |> Super.terminate_child()

        {:reply, :ok, Map.delete(state, client)}

      _ ->
        {:reply, :ok, state}
    end
  end

  @doc """
  Forward execution to flagged Client process
  """
  def handle_call({method, value, db}, _from, state) do
    case Map.has_key?(state, db) && Process.alive?(Map.get(state, db)) do
      true ->
        {:reply, GenServer.call(Map.get(state, db), {method, value}), state}

      _ ->
        case Client.create() do
          {:ok, child} ->
            {:reply, GenServer.call(child, {method, value}), Map.put(state, db, child)}

          _ ->
            {:reply, {:error, :invalid_client_index}, state}
        end
    end
  end

  defp process_integrity("user" = username),
    do: {:ok, username <> ":" <> Conn.realm() <> ":" <> @password}

  defp process_integrity(_),
    do: {:error, :not_found}

  defp process_json(%{"key" => key, "selector" => selector, "delta" => delta}),
    do: [key, selector, delta]

  defp process_json(%{"key" => key, "data" => data}), do: [key, data]
  defp process_json(%{"key" => key}), do: [key]
  defp process_json(_), do: nil
end
