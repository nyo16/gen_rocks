defmodule GenRocks.Storage.DiskLogAdapter do
  @moduledoc """
  Disk-based storage adapter using Erlang's disk_log module.
  
  This adapter uses disk_log as a Write Ahead Log (WAL) system, providing:
  - Durable storage with automatic repair capabilities
  - High-throughput logging with efficient chunk-based reading
  - Wrap log support for continuous operation
  - Multiple reader/writer support
  - Automatic file management and rotation
  
  Perfect for queue systems where durability and performance are critical.
  
  Configuration options:
  - `:log_dir` - Base directory for log files (default: "./gen_rocks_logs")
  - `:topic` - Topic name for organizing logs (required)
  - `:partition` - Partition number (required)
  - `:log_type` - Type of log (:wrap, :halt) (default: :wrap)
  - `:max_no_files` - Maximum number of wrap files (default: 10)
  - `:max_no_bytes` - Maximum bytes per wrap file (default: 10MB)
  - `:repair` - Repair mode (:truncate, false) (default: :truncate)
  - `:format` - Log format (:internal, :external) (default: :internal)
  """
  
  @behaviour GenRocks.Storage.Adapter
  
  require Logger
  
  defstruct [
    :log_name,
    :log_dir,
    :log_type,
    :max_no_files,
    :max_no_bytes,
    :repair,
    :format,
    :topic,
    :partition
  ]
  
  @default_options [
    log_dir: "./gen_rocks_logs",
    log_type: :wrap,
    max_no_files: 10,
    max_no_bytes: 10 * 1024 * 1024,  # 10MB per file
    repair: true,  # Repair if needed but don't truncate good data
    format: :internal
  ]
  
  @impl true
  def open(config) do
    opts = Keyword.merge(@default_options, Enum.into(config, []))
    
    topic = Keyword.get(opts, :topic) || Map.get(config, :topic)
    partition = Keyword.get(opts, :partition, 0) || Map.get(config, :partition, 0)
    log_dir = Keyword.get(opts, :log_dir)
    
    
    if !topic do
      {:error, :missing_topic}
    else
      # Create directory structure: log_dir/topic/partition_X
      topic_dir = Path.join(log_dir, to_string(topic))
      partition_dir = Path.join(topic_dir, "partition_#{partition}")
      
      case File.mkdir_p(partition_dir) do
        :ok ->
          log_name = :"gen_rocks_#{topic}_#{partition}"
          log_file = Path.join(partition_dir, "log")
          
          disk_log_opts = build_disk_log_options(log_name, log_file, opts)
          
          case :disk_log.open(disk_log_opts) do
            {:ok, ^log_name} ->
              storage_ref = %__MODULE__{
                log_name: log_name,
                log_dir: partition_dir,
                log_type: Keyword.get(opts, :log_type),
                max_no_files: Keyword.get(opts, :max_no_files),
                max_no_bytes: Keyword.get(opts, :max_no_bytes),
                repair: Keyword.get(opts, :repair),
                format: Keyword.get(opts, :format),
                topic: topic,
                partition: partition
              }
              
              Logger.info("DiskLogAdapter opened: #{topic}/#{partition} -> #{partition_dir}")
              {:ok, storage_ref}
              
            {:error, reason} ->
              Logger.error("Failed to open disk_log: #{inspect(reason)}")
              {:error, {:disk_log_error, reason}}
          end
          
        {:error, reason} ->
          Logger.error("Failed to create log directory #{partition_dir}: #{inspect(reason)}")
          {:error, {:directory_error, reason}}
      end
    end
  end
  
  @impl true
  def close(storage_ref) do
    case :disk_log.close(storage_ref.log_name) do
      :ok ->
        Logger.info("DiskLogAdapter closed: #{storage_ref.topic}/#{storage_ref.partition}")
        :ok
      {:error, reason} ->
        Logger.error("Failed to close disk_log: #{inspect(reason)}")
        {:error, {:close_error, reason}}
    end
  end
  
  @impl true
  def put(storage_ref, key, value, _options \\ []) do
    # Store as {key, value, timestamp} tuple for WAL semantics
    log_entry = {key, value, System.system_time(:millisecond)}
    
    case :disk_log.log(storage_ref.log_name, log_entry) do
      :ok ->
        :ok
      {:error, reason} ->
        Logger.error("Failed to write to disk_log: #{inspect(reason)}")
        {:error, {:write_error, reason}}
    end
  end
  
  @impl true
  def get(storage_ref, key, _options \\ []) do
    # For WAL semantics, we need to read through the log to find the latest value for key
    case read_latest_value_for_key(storage_ref, key) do
      {:ok, value} -> {:ok, value}
      :not_found -> :not_found
      {:error, reason} -> {:error, reason}
    end
  end
  
  @impl true
  def delete(storage_ref, key, _options \\ []) do
    # In WAL semantics, we don't actually delete, we log a tombstone
    tombstone_entry = {key, :__tombstone__, System.system_time(:millisecond)}
    
    case :disk_log.log(storage_ref.log_name, tombstone_entry) do
      :ok ->
        :ok
      {:error, reason} ->
        Logger.error("Failed to write tombstone to disk_log: #{inspect(reason)}")
        {:error, {:write_error, reason}}
    end
  end
  
  @impl true
  def exists?(storage_ref, key, _options \\ []) do
    case get(storage_ref, key) do
      {:ok, _value} -> true
      :not_found -> false
      {:error, _reason} -> false
    end
  end
  
  @impl true
  def batch(storage_ref, operations, _options \\ []) do
    timestamp = System.system_time(:millisecond)
    
    # Log each operation individually to maintain consistency with single operations
    results = Enum.map(operations, fn operation ->
      log_entry = case operation do
        {:put, key, value} -> {key, value, timestamp}
        {:delete, key} -> {key, :__tombstone__, timestamp}
      end
      
      :disk_log.log(storage_ref.log_name, log_entry)
    end)
    
    # Check if all operations succeeded
    case Enum.find(results, fn result -> result != :ok end) do
      nil -> :ok
      {:error, reason} ->
        Logger.error("Failed to batch write to disk_log: #{inspect(reason)}")
        {:error, {:batch_write_error, reason}}
    end
  end
  
  @impl true
  def scan(storage_ref, prefix, options \\ []) do
    # For scanning, we read all entries and build the current state
    limit = Keyword.get(options, :limit, 1000)
    
    case read_all_current_entries(storage_ref, limit) do
      {:ok, entries} -> 
        # Filter by prefix if specified
        filtered_entries = if prefix do
          Enum.filter(entries, fn {key, _value} ->
            String.starts_with?(to_string(key), to_string(prefix))
          end)
        else
          entries
        end
        {:ok, filtered_entries}
      {:error, reason} -> 
        {:error, reason}
    end
  end
  
  @impl true
  def info(storage_ref) do
    case :disk_log.info(storage_ref.log_name) do
      info_list when is_list(info_list) ->
        {:ok, %{
          adapter: "disk_log",
          topic: storage_ref.topic,
          partition: storage_ref.partition,
          log_dir: storage_ref.log_dir,
          log_type: storage_ref.log_type,
          disk_log_info: Map.new(info_list)
        }}
      {:error, reason} ->
        {:error, {:info_error, reason}}
    end
  end
  
  # Custom functions for WAL-specific operations
  
  @doc """
  Reads all entries from the log in chronological order.
  This is useful for consumer implementations that need to process the WAL.
  """
  def read_log_entries(storage_ref, options \\ []) do
    chunk_size = Keyword.get(options, :chunk_size, 100)
    
    case :disk_log.chunk(storage_ref.log_name, :start, chunk_size) do
      :eof ->
        {:ok, []}
      {:error, reason} ->
        {:error, {:read_error, reason}}
      {continuation, terms} ->
        read_log_entries_continue(storage_ref, continuation, terms, chunk_size)
    end
  end
  
  @doc """
  Truncates the log, removing all entries.
  Use with caution - this will remove all data!
  """
  def truncate(storage_ref) do
    case :disk_log.truncate(storage_ref.log_name) do
      :ok ->
        Logger.warning("DiskLog truncated: #{storage_ref.topic}/#{storage_ref.partition}")
        :ok
      {:error, reason} ->
        {:error, {:truncate_error, reason}}
    end
  end
  
  @doc """
  Synchronizes the log to disk, ensuring all data is written.
  """
  def sync(storage_ref) do
    case :disk_log.sync(storage_ref.log_name) do
      :ok ->
        :ok
      {:error, reason} ->
        Logger.error("Disk_log sync failed: #{inspect(reason)}")
        {:error, {:sync_error, reason}}
    end
  end
  
  # Private functions
  
  defp build_disk_log_options(log_name, log_file, opts) do
    base_opts = [
      name: log_name,
      file: to_charlist(log_file),
      repair: Keyword.get(opts, :repair),
      format: Keyword.get(opts, :format)
    ]
    
    case Keyword.get(opts, :log_type) do
      :wrap ->
        base_opts ++ [
          type: :wrap,
          size: {Keyword.get(opts, :max_no_bytes), Keyword.get(opts, :max_no_files)}
        ]
      :halt ->
        base_opts ++ [
          type: :halt,
          size: Keyword.get(opts, :max_no_bytes)
        ]
      _ ->
        base_opts ++ [type: :wrap, size: {Keyword.get(opts, :max_no_bytes), Keyword.get(opts, :max_no_files)}]
    end
  end
  
  defp read_latest_value_for_key(storage_ref, target_key) do
    case read_all_entries_reversed(storage_ref) do
      {:ok, entries} ->
        # Flatten entries to handle both individual and batch entries
        flattened_entries = entries
        |> Enum.flat_map(fn entry ->
          case entry do
            {_key, _value, _timestamp} = tuple -> [tuple]
            list when is_list(list) -> list
            other -> [other]
          end
        end)
        
        # Find the latest entry for this key (could be tombstone or value)
        # Since entries are in reverse chronological order (latest first), 
        # we want the first match
        case Enum.find(flattened_entries, fn
          {^target_key, _, _timestamp} -> true
          _ -> false
        end) do
          {^target_key, :__tombstone__, _timestamp} -> :not_found
          {^target_key, value, _timestamp} -> {:ok, value}
          nil -> :not_found
        end
      {:error, reason} -> 
        {:error, reason}
    end
  end
  
  defp read_all_current_entries(storage_ref, limit) do
    case read_all_entries_reversed(storage_ref) do
      {:ok, entries} ->
        # Build current state by processing entries in reverse chronological order
        # Handle both individual entries and lists of entries from batch operations
        current_state = entries
        |> Enum.flat_map(fn entry ->
          case entry do
            # Individual entry 
            {_key, _value, _timestamp} = tuple -> [tuple]
            # List of entries from batch operation
            list when is_list(list) -> list
            # Handle any other format
            other -> [other]
          end
        end)
        |> Enum.reduce(%{}, fn
          {key, :__tombstone__, _timestamp}, acc -> 
            # Only delete if we haven't seen this key yet (latest wins)
            if Map.has_key?(acc, key) do
              acc
            else
              Map.put(acc, key, :__deleted__)
            end
          {key, value, _timestamp}, acc -> 
            # Only keep if we haven't seen this key yet (latest wins)
            if Map.has_key?(acc, key) do
              acc
            else
              Map.put(acc, key, value)
            end
        end)
        |> Enum.filter(fn {_key, value} -> value != :__deleted__ end)
        |> Enum.take(limit)
        
        {:ok, current_state}
      {:error, reason} -> 
        {:error, reason}
    end
  end
  
  defp read_all_entries_reversed(storage_ref) do
    case read_log_entries(storage_ref) do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, reason} -> {:error, reason}
    end
  end
  
  defp read_log_entries_continue(storage_ref, continuation, acc, chunk_size) do
    case :disk_log.chunk(storage_ref.log_name, continuation, chunk_size) do
      :eof ->
        {:ok, acc}
      {:error, reason} ->
        {:error, {:read_error, reason}}
      {new_continuation, terms} ->
        read_log_entries_continue(storage_ref, new_continuation, acc ++ terms, chunk_size)
    end
  end
end