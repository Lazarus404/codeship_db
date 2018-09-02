defmodule CodeshipDB.Store.Client do
  @moduledoc """
  Client process, identifiable by name
  """
  use GenServer
  alias CodeshipDB.Store.{Manager, Super}

  @spec start_link() :: :ok
  def start_link() do
    GenServer.start_link(__MODULE__, [])
  end

  @spec create() :: {:ok, map()}
  def create() do
    Super.start_child()
  end

  @spec destroy(pid() | String.t()) :: :ok
  def destroy(pid) when is_pid(pid) do
    Super.terminate_child(pid)
  end

  def destroy(bucket) do
    Manager.do_execute(:destroy, nil, to_string(bucket))
  end

  @spec has_bucket?(String.t()) :: boolean()
  def has_bucket?(bucket) do
    Manager.do_execute(:bucket_exists, nil, to_string(bucket))
  end

  @spec get(String.t(), String.t()) :: {:ok, term()} | {:error, String.t()}
  def get(bucket, key) do
    Manager.do_execute(:get, [key], to_string(bucket))
  end

  @spec set(String.t(), String.t(), term()) :: :ok
  def set(bucket, key, val) do
    Manager.do_execute(:set, [key, val], to_string(bucket))
  end

  @spec update(String.t(), String.t(), map(), map()) :: :ok | {:error, term()}
  def update(bucket, key, selector, delta \\ %{}) do
    Manager.do_execute(:update, [key, selector, delta], to_string(bucket))
  end

  @spec delete(String.t(), String.t()) :: {:ok, integer()}
  def delete(bucket, key) do
    Manager.do_execute(:del, [key], to_string(bucket))
  end

  @spec delete_all(String.t()) :: {:ok, integer()}
  def delete_all(bucket) do
    Manager.do_execute(:del_all, nil, to_string(bucket))
  end

  #########################################################################################################################
  # OTP functions
  #########################################################################################################################

  def init([]) do
    {:ok, %{client: %{}}}
  end

  def handle_call({:get, [key]}, _from, %{client: client} = state) do
    case Map.get(client, key, nil) do
      nil -> {:reply, {:error, "does not exist"}, state}
      val -> {:reply, {:ok, val}, state}
    end
  end

  def handle_call({:set, [key, value]}, _from, %{client: client} = state) do
    {:reply, :ok, %{state | client: Map.put(client, key, value)}}
  end

  def handle_call({:update, [key, selector, delta]}, _from, %{client: client} = state) do
    value =
      client
      |> get_list(key)
      |> modify(selector, delta)

    {:reply, :ok, %{state | client: Map.put(client, key, value)}}
  end

  def handle_call({:del, [key]}, _from, %{client: client} = state) do
    case Map.has_key?(client, key) do
      true ->
        {:reply, {:ok, 1}, %{state | client: Map.delete(client, key)}}

      false ->
        {:reply, {:ok, 0}, %{state | client: Map.delete(client, key)}}
    end
  end

  def handle_call({:del_all, _}, _from, state) do
    {:reply, {:ok, 1}, %{state | client: %{}}}
  end

  def handle_call({a, b}, _from, state) do
    {:reply, {:error, "#{a}, #{b}"}, state}
  end

  ##
  ## Private functions
  ##

  ##  Returns a given item if it exists and is
  ##  a list. Else, an empty list is returned
  @spec get_list(map(), String.t()) :: list()
  defp get_list(client, key) do
    case Map.get(client, key, nil) do
      val when is_list(val) -> val
      _ -> []
    end
  end

  @spec modify(list(), map(), map()) :: list()
  defp modify(collection, selector, delta) do
    collection
    |> find(selector, delta)
    |> incr(delta)
    |> combine()
  end

  defp find(collection, selector, %{"$setOnInsert" => insert}) do
    case Enum.reduce(collection, {[], []}, &has_values?(&1, &2, selector)) do
      {[], others} -> {[insert], others}
      val -> val
    end
  end

  defp find(collection, selector, _),
    do: Enum.reduce(collection, {[], []}, &has_values?(&1, &2, selector))

  defp incr({[], _} = val, _), do: val

  defp incr({items, others}, %{"$inc" => inc}) do
    updated =
      Enum.map(items, fn i ->
        vals =
          for {k, v} <- inc, into: %{} do
            {k, Map.get(i, k, initial_val(v)) + v}
          end

        Map.merge(i, vals)
      end)

    {updated, others}
  end

  defp incr(items, _), do: items

  defp combine({a, b}), do: List.flatten([a, b])
  defp combine(a), do: a

  defp initial_val(v) when is_integer(v), do: 0
  defp initial_val(v) when is_float(v), do: 0.0

  defp has_values?(item, {found, others}, selector) do
    cond do
      Map.merge(item, selector) == item and
          Map.keys(selector) |> Enum.all?(&Map.has_key?(item, &1)) ->
        {List.flatten([item, found]), others}

      true ->
        {found, List.flatten([item, others])}
    end
  end
end
