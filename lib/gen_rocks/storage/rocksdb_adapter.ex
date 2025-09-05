defmodule GenRocks.Storage.RocksdbAdapter do
  @moduledoc """
  RocksDB-based storage adapter for persistent queue storage.
  Provides durability and high performance for production deployments.
  """

  @behaviour GenRocks.Storage.Adapter

  require Logger

  @impl true
  def open(config) do
    db_path = Map.get(config, :path, "data/rocksdb")
    
    # Default RocksDB options optimized for queue workloads
    default_options = [
      create_if_missing: true,
      max_open_files: 100,
      write_buffer_size: 64 * 1024 * 1024,  # 64MB
      max_write_buffer_number: 3,
      target_file_size_base: 64 * 1024 * 1024,  # 64MB
      level0_file_num_compaction_trigger: 4,
      level0_slowdown_writes_trigger: 20,
      level0_stop_writes_trigger: 36,
      max_background_compactions: 4,
      max_background_flushes: 2,
      compression: :lz4
    ]
    
    options = Keyword.merge(default_options, Map.get(config, :options, []))
    
    # Ensure directory exists
    File.mkdir_p!(Path.dirname(db_path))
    
    case :rocksdb.open(db_path, options) do
      {:ok, db} ->
        Logger.info("RocksDB opened at: #{db_path}")
        {:ok, %{db: db, path: db_path, config: config}}
      
      {:error, reason} ->
        Logger.error("Failed to open RocksDB at #{db_path}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @impl true
  def close(storage_ref) do
    case :rocksdb.close(storage_ref.db) do
      :ok ->
        Logger.info("RocksDB closed: #{storage_ref.path}")
        :ok
      
      {:error, reason} ->
        Logger.error("Failed to close RocksDB: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @impl true
  def put(storage_ref, key, value, options \\ []) do
    write_options = build_write_options(options)
    serialized_value = serialize_value(value)
    
    case :rocksdb.put(storage_ref.db, key, serialized_value, write_options) do
      :ok -> :ok
      {:error, reason} ->
        Logger.error("RocksDB put failed for key #{key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @impl true
  def get(storage_ref, key, options \\ []) do
    read_options = build_read_options(options)
    
    case :rocksdb.get(storage_ref.db, key, read_options) do
      {:ok, serialized_value} ->
        {:ok, deserialize_value(serialized_value)}
      
      :not_found ->
        :not_found
      
      {:error, reason} ->
        Logger.error("RocksDB get failed for key #{key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @impl true
  def delete(storage_ref, key, options \\ []) do
    write_options = build_write_options(options)
    
    case :rocksdb.delete(storage_ref.db, key, write_options) do
      :ok -> :ok
      {:error, reason} ->
        Logger.error("RocksDB delete failed for key #{key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @impl true
  def exists?(storage_ref, key, options \\ []) do
    case get(storage_ref, key, options) do
      {:ok, _} -> true
      :not_found -> false
      {:error, _} = error -> error
    end
  end

  @impl true
  def batch(storage_ref, operations, options \\ []) do
    write_options = build_write_options(options)
    
    try do
      {:ok, batch} = :rocksdb.batch()
      
      Enum.each(operations, fn operation ->
        case operation do
          {:put, key, value} ->
            serialized_value = serialize_value(value)
            :rocksdb.batch_put(batch, key, serialized_value)
          
          {:delete, key} ->
            :rocksdb.batch_delete(batch, key)
        end
      end)
      
      case :rocksdb.write_batch(storage_ref.db, batch, write_options) do
        :ok -> 
          :rocksdb.release_batch(batch)
          :ok
        
        {:error, reason} -> 
          :rocksdb.release_batch(batch)
          Logger.error("RocksDB batch write failed: #{inspect(reason)}")
          {:error, reason}
      end
    rescue
      error ->
        Logger.error("RocksDB batch operation failed: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def scan(storage_ref, prefix, options \\ []) do
    limit = Keyword.get(options, :limit, :infinity)
    read_options = build_read_options(options)
    
    try do
      Stream.resource(
        fn ->
          case :rocksdb.iterator(storage_ref.db, read_options) do
            {:ok, iter} ->
              start_key = prefix || ""
              case :rocksdb.iterator_move(iter, {:seek, start_key}) do
                {:ok, key, value} when is_nil(prefix) or binary_part(key, 0, byte_size(prefix)) == prefix ->
                  {iter, {key, value}, :continue}
                {:ok, _key, _value} ->
                  # Key doesn't match prefix
                  :rocksdb.iterator_close(iter)
                  {nil, nil, :done}
                {:error, :invalid_iterator} ->
                  :rocksdb.iterator_close(iter)
                  {nil, nil, :done}
              end
            
            {:error, reason} ->
              Logger.error("Failed to create RocksDB iterator: #{inspect(reason)}")
              {nil, nil, :error}
          end
        end,
        fn
          {nil, nil, :done} -> {:halt, nil}
          {nil, nil, :error} -> {:halt, nil}
          {iter, {key, value}, :continue} ->
            # Check if key still matches prefix
            if is_nil(prefix) or binary_part(key, 0, min(byte_size(key), byte_size(prefix))) == prefix do
              deserialized_value = deserialize_value(value)
              
              # Get next item
              next_state = case :rocksdb.iterator_move(iter, :next) do
                {:ok, next_key, next_value} when is_nil(prefix) or binary_part(next_key, 0, byte_size(prefix)) == prefix ->
                  {iter, {next_key, next_value}, :continue}
                
                {:ok, _next_key, _next_value} ->
                  # Next key doesn't match prefix
                  :rocksdb.iterator_close(iter)
                  {nil, nil, :done}
                
                {:error, :invalid_iterator} ->
                  :rocksdb.iterator_close(iter)
                  {nil, nil, :done}
              end
              
              {[{key, deserialized_value}], next_state}
            else
              # Key doesn't match prefix anymore
              :rocksdb.iterator_close(iter)
              {:halt, nil}
            end
        end,
        fn
          {iter, _, _} when not is_nil(iter) -> :rocksdb.iterator_close(iter)
          _ -> :ok
        end
      )
      |> maybe_limit(limit)
    rescue
      error ->
        Logger.error("RocksDB scan failed: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def info(storage_ref) do
    try do
      # Get RocksDB property information
      properties = [
        "rocksdb.estimate-num-keys",
        "rocksdb.total-sst-files-size", 
        "rocksdb.estimate-table-readers-mem",
        "rocksdb.cur-size-all-mem-tables"
      ]
      
      stats = Enum.reduce(properties, %{}, fn prop, acc ->
        case :rocksdb.get_property(storage_ref.db, prop) do
          {:ok, value} -> Map.put(acc, prop, value)
          _ -> acc
        end
      end)
      
      Map.merge(stats, %{
        path: storage_ref.path,
        adapter: __MODULE__
      })
    rescue
      error ->
        Logger.error("RocksDB info failed: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def optimize(storage_ref, options) do
    case Keyword.get(options, :operation) do
      :compact ->
        # Trigger manual compaction
        case :rocksdb.compact_range(storage_ref.db, nil, nil, []) do
          :ok -> 
            Logger.info("RocksDB compaction completed")
            :ok
          {:error, reason} -> 
            Logger.error("RocksDB compaction failed: #{inspect(reason)}")
            {:error, reason}
        end
      
      :flush ->
        # Force flush of memtables
        case :rocksdb.flush(storage_ref.db, []) do
          :ok ->
            Logger.info("RocksDB flush completed")
            :ok
          {:error, reason} ->
            Logger.error("RocksDB flush failed: #{inspect(reason)}")
            {:error, reason}
        end
      
      _ ->
        Logger.info("RocksDB optimization: no specific operation requested")
        :ok
    end
  end

  # Private helper functions

  defp serialize_value(value) do
    :erlang.term_to_binary(value, [compressed: 1])
  end

  defp deserialize_value(serialized_value) do
    :erlang.binary_to_term(serialized_value)
  end

  defp build_write_options(options) do
    # Convert common options to RocksDB format
    Keyword.take(options, [:sync, :disable_wal])
  end

  defp build_read_options(options) do
    # Convert common options to RocksDB format
    Keyword.take(options, [:verify_checksums, :fill_cache])
  end

  defp maybe_limit(stream, :infinity), do: stream
  defp maybe_limit(stream, limit) when is_integer(limit) and limit > 0 do
    Stream.take(stream, limit)
  end
  defp maybe_limit(stream, _), do: stream
end