defmodule GenRocks.Storage.EtsAdapter do
  @moduledoc """
  ETS-based storage adapter for in-memory queue storage.
  Fast and suitable for POCs and single-node deployments.
  """

  @behaviour GenRocks.Storage.Adapter

  require Logger

  @impl true
  def open(config) do
    table_name = Map.get(config, :table_name, :gen_rocks_storage)
    table_options = [
      :set,
      :public,
      :named_table,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ] ++ Map.get(config, :table_options, [])

    try do
      case :ets.whereis(table_name) do
        :undefined ->
          table = :ets.new(table_name, table_options)
          Logger.info("ETS table created: #{table_name}")
          {:ok, %{table: table, name: table_name, config: config}}
        
        existing_table ->
          Logger.info("ETS table already exists: #{table_name}")
          {:ok, %{table: existing_table, name: table_name, config: config}}
      end
    rescue
      error ->
        Logger.error("Failed to create ETS table: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def close(storage_ref) do
    try do
      if :ets.whereis(storage_ref.name) != :undefined do
        :ets.delete(storage_ref.table)
        Logger.info("ETS table deleted: #{storage_ref.name}")
      end
      :ok
    rescue
      error ->
        Logger.error("Failed to close ETS table: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def put(storage_ref, key, value, _options \\ []) do
    try do
      # Serialize value to ensure consistent storage format
      serialized_value = serialize_value(value)
      :ets.insert(storage_ref.table, {key, serialized_value})
      :ok
    rescue
      error ->
        Logger.error("ETS put failed for key #{key}: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def get(storage_ref, key, _options \\ []) do
    try do
      case :ets.lookup(storage_ref.table, key) do
        [{^key, serialized_value}] ->
          {:ok, deserialize_value(serialized_value)}
        [] ->
          :not_found
      end
    rescue
      error ->
        Logger.error("ETS get failed for key #{key}: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def delete(storage_ref, key, _options \\ []) do
    try do
      :ets.delete(storage_ref.table, key)
      :ok
    rescue
      error ->
        Logger.error("ETS delete failed for key #{key}: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def exists?(storage_ref, key, _options \\ []) do
    try do
      case :ets.lookup(storage_ref.table, key) do
        [] -> false
        [_] -> true
      end
    rescue
      error ->
        Logger.error("ETS exists? failed for key #{key}: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def batch(storage_ref, operations, _options \\ []) do
    try do
      # ETS doesn't have native batch operations, so we simulate it
      # This isn't truly atomic, but it's fast for in-memory operations
      Enum.each(operations, fn operation ->
        case operation do
          {:put, key, value} ->
            serialized_value = serialize_value(value)
            :ets.insert(storage_ref.table, {key, serialized_value})
          
          {:delete, key} ->
            :ets.delete(storage_ref.table, key)
        end
      end)
      :ok
    rescue
      error ->
        Logger.error("ETS batch operation failed: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def scan(storage_ref, prefix, options \\ []) do
    limit = Keyword.get(options, :limit, :infinity)
    
    try do
      pattern = if prefix, do: {prefix <> "$1", :"$2"}, else: {:"$1", :"$2"}
      
      Stream.resource(
        fn -> 
          case :ets.match(storage_ref.table, pattern, 100) do
            {matches, continuation} -> {matches, continuation}
            :"$end_of_table" -> {[], nil}
          end
        end,
        fn
          {[], nil} -> {:halt, nil}
          {matches, continuation} ->
            processed_matches = 
              matches
              |> Enum.map(fn [key_part, value] ->
                full_key = if prefix, do: prefix <> key_part, else: key_part
                {full_key, deserialize_value(value)}
              end)
            
            next_state = case continuation do
              nil -> {[], nil}
              cont -> 
                case :ets.match(cont) do
                  {next_matches, next_continuation} -> {next_matches, next_continuation}
                  :"$end_of_table" -> {[], nil}
                end
            end
            
            {processed_matches, next_state}
        end,
        fn _ -> :ok end
      )
      |> maybe_limit(limit)
    rescue
      error ->
        Logger.error("ETS scan failed: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def info(storage_ref) do
    try do
      info = :ets.info(storage_ref.table)
      %{
        size: info[:size],
        memory: info[:memory],
        type: info[:type],
        protection: info[:protection],
        name: storage_ref.name,
        adapter: __MODULE__
      }
    rescue
      error ->
        Logger.error("ETS info failed: #{inspect(error)}")
        {:error, error}
    end
  end

  # Optional optimization callback
  @impl true
  def optimize(storage_ref, options) do
    # ETS doesn't need much optimization, but we can provide some utilities
    case Keyword.get(options, :operation) do
      :compact ->
        # ETS automatically manages memory, but we can provide stats
        info = info(storage_ref)
        Logger.info("ETS optimization requested. Current stats: #{inspect(info)}")
        :ok
      
      :clear ->
        :ets.delete_all_objects(storage_ref.table)
        Logger.info("ETS table cleared: #{storage_ref.name}")
        :ok
      
      _ ->
        Logger.info("ETS optimization: no specific operation requested")
        :ok
    end
  end

  # Private helper functions

  defp serialize_value(value) do
    # Use Erlang term serialization for consistent format
    :erlang.term_to_binary(value, [compressed: 1])
  end

  defp deserialize_value(serialized_value) do
    :erlang.binary_to_term(serialized_value)
  end

  defp maybe_limit(stream, :infinity), do: stream
  defp maybe_limit(stream, limit) when is_integer(limit) and limit > 0 do
    Stream.take(stream, limit)
  end
  defp maybe_limit(stream, _), do: stream
end