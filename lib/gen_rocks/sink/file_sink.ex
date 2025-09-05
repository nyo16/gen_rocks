defmodule GenRocks.Sink.FileSink do
  @moduledoc """
  File-based sink implementation for writing data to files.
  Supports various output formats and writing patterns.
  
  Supports:
  - Line-by-line writing (text files, logs)
  - JSON files (one JSON object per line or JSON array)
  - CSV files with headers
  - Custom formatters via callback functions
  - File rotation based on size or time
  - Compression of output files
  """

  @behaviour GenRocks.Sink

  require Logger
  alias GenRocks.Queue.Message

  defstruct [
    :file_handle,
    :file_path,
    :format,
    :formatter,
    :encoding,
    :rotation_size,
    :rotation_time,
    :current_size,
    :created_at,
    :messages_written,
    :compress,
    :file_counter,
    :csv_headers,
    :csv_headers_written,
    :json_array_mode,
    :json_first_item
  ]

  @doc """
  Configuration options:
  
  - `:file_path` - Path where to write the file
  - `:format` - Output format (:lines, :json, :json_array, :csv, :custom)
  - `:formatter` - Custom formatter function for :custom format
  - `:encoding` - File encoding (default: :utf8)
  - `:append` - Whether to append to existing file (default: false)
  - `:rotation_size` - Rotate file when it exceeds this size in bytes
  - `:rotation_time` - Rotate file after this time in seconds
  - `:compress` - Compress rotated files (default: false)
  - `:csv_headers` - List of CSV headers (required for CSV format)
  - `:json_array_mode` - For JSON format, write as array instead of JSONL
  """
  @impl true
  def init(config) do
    file_path = Map.get(config, :file_path)
    format = Map.get(config, :format, :lines)
    formatter = Map.get(config, :formatter)
    encoding = Map.get(config, :encoding, :utf8)
    append = Map.get(config, :append, false)
    rotation_size = Map.get(config, :rotation_size)
    rotation_time = Map.get(config, :rotation_time)
    compress = Map.get(config, :compress, false)
    csv_headers = Map.get(config, :csv_headers)
    json_array_mode = Map.get(config, :json_array_mode, false)

    if not file_path do
      {:error, :missing_file_path}
    else
      # Ensure directory exists
      file_path |> Path.dirname() |> File.mkdir_p!()

      # Determine file open mode
      open_mode = if append, do: [:append, encoding], else: [:write, encoding]

      case File.open(file_path, open_mode) do
        {:ok, handle} ->
          state = %__MODULE__{
            file_handle: handle,
            file_path: file_path,
            format: format,
            formatter: formatter,
            encoding: encoding,
            rotation_size: rotation_size,
            rotation_time: rotation_time,
            current_size: if(append, do: get_file_size(file_path), else: 0),
            created_at: System.monotonic_time(:second),
            messages_written: 0,
            compress: compress,
            file_counter: 0,
            csv_headers: csv_headers,
            csv_headers_written: append,  # Assume headers exist if appending
            json_array_mode: json_array_mode,
            json_first_item: true
          }

          # Write initial content based on format
          new_state = case format do
            :csv when not append and csv_headers ->
              write_csv_headers(state)
            :json_array when not append ->
              write_json_array_start(state)
            _ -> state
          end

          Logger.info("FileSink initialized: #{file_path} (format: #{format})")
          {:ok, new_state}

        {:error, reason} ->
          Logger.error("Failed to open file #{file_path}: #{inspect(reason)}")
          {:error, reason}
      end
    end
  end

  @impl true
  def write_batch(state, messages) do
    try do
      new_state = Enum.reduce(messages, state, fn message, acc_state ->
        case write_single_message(acc_state, message) do
          {:ok, updated_state} -> updated_state
          {:error, reason} -> 
            Logger.error("Failed to write message: #{inspect(reason)}")
            acc_state
        end
      end)

      # Check if file rotation is needed
      final_state = check_and_rotate_if_needed(new_state)

      {:ok, final_state}
    rescue
      error ->
        Logger.error("Exception during batch write: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def flush(state) do
    case :file.sync(state.file_handle) do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def close(state) do
    # Write closing content based on format
    final_state = case state.format do
      :json_array -> write_json_array_end(state)
      _ -> state
    end

    if final_state.file_handle do
      File.close(final_state.file_handle)
    end
    
    Logger.info("FileSink closed, wrote #{final_state.messages_written} messages")
    :ok
  end

  @impl true
  def info(state) do
    %{
      sink_type: "file",
      file_path: state.file_path,
      format: state.format,
      messages_written: state.messages_written,
      current_size: state.current_size,
      created_at: state.created_at,
      rotation_size: state.rotation_size,
      rotation_time: state.rotation_time
    }
  end

  # Private functions

  defp write_single_message(state, message) do
    case format_message(state, message) do
      {:ok, formatted_content} ->
        case IO.write(state.file_handle, formatted_content) do
          :ok ->
            new_state = %{state |
              messages_written: state.messages_written + 1,
              current_size: state.current_size + byte_size(formatted_content)
            }
            {:ok, new_state}
          
          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp format_message(state, message) do
    case state.format do
      :lines -> format_lines(state, message)
      :json -> format_json(state, message)
      :json_array -> format_json_array(state, message)
      :csv -> format_csv(state, message)
      :custom -> format_custom(state, message)
    end
  end

  defp format_lines(_state, message) do
    content = case message.value do
      binary when is_binary(binary) -> binary
      other -> inspect(other)
    end
    {:ok, content <> "\n"}
  end

  defp format_json(_state, message) do
    # JSONL format - one JSON object per line
    json_object = %{
      key: message.key,
      value: message.value,
      timestamp: message.timestamp,
      headers: message.headers,
      metadata: message.metadata
    }
    
    case Jason.encode(json_object) do
      {:ok, json_string} -> {:ok, json_string <> "\n"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp format_json_array(state, message) do
    # JSON array format
    json_object = %{
      key: message.key,
      value: message.value,
      timestamp: message.timestamp,
      headers: message.headers,
      metadata: message.metadata
    }
    
    case Jason.encode(json_object) do
      {:ok, json_string} -> 
        prefix = if state.json_first_item do
          # Update state to mark first item as written
          send(self(), :mark_first_json_written)
          ""
        else
          ","
        end
        {:ok, prefix <> json_string}
      
      {:error, reason} -> 
        {:error, reason}
    end
  end

  defp format_csv(state, message) do
    if state.csv_headers do
      # Extract values based on headers
      values = Enum.map(state.csv_headers, fn header ->
        case message.value do
          map when is_map(map) -> 
            Map.get(map, header) || Map.get(map, to_string(header)) || ""
          _ -> ""
        end
      end)

      csv_line = values
      |> Enum.map(&escape_csv_field/1)
      |> Enum.join(",")
      
      {:ok, csv_line <> "\n"}
    else
      {:error, :missing_csv_headers}
    end
  end

  defp format_custom(state, message) do
    if state.formatter && is_function(state.formatter, 2) do
      try do
        case state.formatter.(message, state) do
          {:ok, formatted} -> {:ok, formatted}
          :skip -> {:ok, ""}
          {:error, reason} -> {:error, reason}
        end
      rescue
        error -> {:error, error}
      end
    else
      {:error, :no_formatter_function}
    end
  end

  defp write_csv_headers(state) do
    if state.csv_headers do
      header_line = state.csv_headers
      |> Enum.map(&to_string/1)
      |> Enum.map(&escape_csv_field/1)
      |> Enum.join(",")
      
      case IO.write(state.file_handle, header_line <> "\n") do
        :ok -> 
          %{state | 
            csv_headers_written: true,
            current_size: state.current_size + byte_size(header_line) + 1
          }
        {:error, _reason} -> 
          state
      end
    else
      state
    end
  end

  defp write_json_array_start(state) do
    case IO.write(state.file_handle, "[") do
      :ok -> 
        %{state | current_size: state.current_size + 1}
      {:error, _reason} -> 
        state
    end
  end

  defp write_json_array_end(state) do
    case IO.write(state.file_handle, "]") do
      :ok -> 
        %{state | current_size: state.current_size + 1}
      {:error, _reason} -> 
        state
    end
  end

  defp escape_csv_field(field) do
    str_field = to_string(field)
    
    if String.contains?(str_field, [",", "\"", "\n", "\r"]) do
      escaped = String.replace(str_field, "\"", "\"\"")
      "\"#{escaped}\""
    else
      str_field
    end
  end

  defp check_and_rotate_if_needed(state) do
    should_rotate = cond do
      state.rotation_size && state.current_size >= state.rotation_size -> 
        true
      state.rotation_time && 
        System.monotonic_time(:second) - state.created_at >= state.rotation_time -> 
        true
      true -> 
        false
    end

    if should_rotate do
      rotate_file(state)
    else
      state
    end
  end

  defp rotate_file(state) do
    Logger.info("Rotating file: #{state.file_path}")

    # Close current file with proper ending
    final_state = case state.format do
      :json_array -> write_json_array_end(state)
      _ -> state
    end

    # Close file handle
    File.close(final_state.file_handle)

    # Generate rotated filename
    timestamp = DateTime.utc_now() |> DateTime.to_iso8601(:basic)
    extension = Path.extname(state.file_path)
    base_name = Path.rootname(state.file_path)
    rotated_path = "#{base_name}.#{timestamp}#{extension}"

    # Move current file to rotated name
    case File.rename(state.file_path, rotated_path) do
      :ok ->
        # Compress if requested
        if state.compress do
          spawn(fn -> compress_file(rotated_path) end)
        end

        # Open new file
        case File.open(state.file_path, [:write, state.encoding]) do
          {:ok, new_handle} ->
            new_state = %{final_state |
              file_handle: new_handle,
              current_size: 0,
              created_at: System.monotonic_time(:second),
              file_counter: final_state.file_counter + 1,
              json_first_item: true
            }

            # Write initial content for new file
            final_new_state = case state.format do
              :csv when state.csv_headers -> write_csv_headers(new_state)
              :json_array -> write_json_array_start(new_state)
              _ -> new_state
            end

            Logger.info("File rotated successfully: #{rotated_path}")
            final_new_state

          {:error, reason} ->
            Logger.error("Failed to open new file after rotation: #{inspect(reason)}")
            final_state
        end

      {:error, reason} ->
        Logger.error("Failed to rotate file: #{inspect(reason)}")
        final_state
    end
  end

  defp compress_file(file_path) do
    Logger.info("Compressing rotated file: #{file_path}")
    
    try do
      {:ok, content} = File.read(file_path)
      compressed = :zlib.gzip(content)
      compressed_path = file_path <> ".gz"
      
      case File.write(compressed_path, compressed) do
        :ok ->
          File.rm(file_path)
          Logger.info("File compressed successfully: #{compressed_path}")
        {:error, reason} ->
          Logger.error("Failed to write compressed file: #{inspect(reason)}")
      end
    rescue
      error ->
        Logger.error("Error compressing file: #{inspect(error)}")
    end
  end

  defp get_file_size(file_path) do
    case File.stat(file_path) do
      {:ok, %{size: size}} -> size
      {:error, _} -> 0
    end
  end
end