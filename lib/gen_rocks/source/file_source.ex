defmodule GenRocks.Source.FileSource do
  @moduledoc """
  File-based source implementation for reading data from files.
  Supports various file formats and reading patterns.
  
  Supports:
  - Line-by-line reading (text files, logs, CSV)
  - JSON files (one JSON object per line or single JSON array)
  - Custom parsers via callback functions
  - Watching files for new content (tail -f style)
  - Reading from multiple files with glob patterns
  """

  @behaviour GenRocks.Source

  require Logger
  alias GenRocks.Queue.Message

  defstruct [
    :file_handle,
    :file_path,
    :format,
    :parser,
    :topic,
    :line_number,
    :bytes_read,
    :watch_mode,
    :glob_pattern,
    :current_files,
    :file_index,
    :metadata
  ]

  @doc """
  Configuration options:
  
  - `:file_path` - Path to the file to read
  - `:glob_pattern` - Glob pattern for multiple files (alternative to file_path)
  - `:format` - Format of the file (:lines, :json, :csv, :custom)
  - `:parser` - Custom parser function for :custom format
  - `:topic` - Topic name for generated messages
  - `:watch_mode` - Whether to watch file for new content (default: false)
  - `:encoding` - File encoding (default: :utf8)
  - `:skip_lines` - Number of lines to skip at the beginning (default: 0)
  """
  @impl true
  def init(config) do
    file_path = Map.get(config, :file_path)
    glob_pattern = Map.get(config, :glob_pattern)
    format = Map.get(config, :format, :lines)
    topic = Map.get(config, :topic, "file_data")
    watch_mode = Map.get(config, :watch_mode, false)
    parser = Map.get(config, :parser)
    encoding = Map.get(config, :encoding, :utf8)
    skip_lines = Map.get(config, :skip_lines, 0)

    cond do
      file_path && File.exists?(file_path) ->
        case File.open(file_path, [:read, encoding]) do
          {:ok, handle} ->
            # Skip initial lines if requested
            skip_initial_lines(handle, skip_lines)
            
            state = %__MODULE__{
              file_handle: handle,
              file_path: file_path,
              format: format,
              parser: parser,
              topic: topic,
              line_number: skip_lines,
              bytes_read: 0,
              watch_mode: watch_mode,
              metadata: %{encoding: encoding}
            }
            
            Logger.info("FileSource initialized: #{file_path} (format: #{format})")
            {:ok, state}

          {:error, reason} ->
            Logger.error("Failed to open file #{file_path}: #{inspect(reason)}")
            {:error, reason}
        end

      glob_pattern ->
        files = Path.wildcard(glob_pattern) |> Enum.sort()
        
        if length(files) > 0 do
          state = %__MODULE__{
            glob_pattern: glob_pattern,
            current_files: files,
            file_index: 0,
            format: format,
            parser: parser,
            topic: topic,
            line_number: 0,
            bytes_read: 0,
            watch_mode: false,  # Watch mode not supported with glob patterns
            metadata: %{total_files: length(files)}
          }
          
          # Open first file
          case open_next_file(state) do
            {:ok, new_state} ->
              Logger.info("FileSource initialized with glob: #{glob_pattern} (#{length(files)} files)")
              {:ok, new_state}
            {:error, reason} -> 
              {:error, reason}
          end
        else
          {:error, :no_files_found}
        end

      file_path ->
        {:error, :file_not_found}

      true ->
        {:error, :missing_file_path_or_glob}
    end
  end

  @impl true
  def read_batch(state, batch_size) do
    read_batch_from_current_file(state, batch_size, [])
  end

  @impl true
  def close(state) do
    if state.file_handle do
      File.close(state.file_handle)
    end
    Logger.info("FileSource closed")
    :ok
  end

  @impl true
  def info(state) do
    %{
      source_type: "file",
      file_path: state.file_path || state.glob_pattern,
      format: state.format,
      line_number: state.line_number,
      bytes_read: state.bytes_read,
      watch_mode: state.watch_mode,
      metadata: state.metadata || %{}
    }
  end

  # Private functions

  defp read_batch_from_current_file(state, batch_size, acc) when batch_size <= 0 do
    {{:ok, Enum.reverse(acc)}, state}
  end

  defp read_batch_from_current_file(state, batch_size, acc) do
    case read_single_item(state) do
      {:ok, message, new_state} ->
        read_batch_from_current_file(new_state, batch_size - 1, [message | acc])

      {:eof, new_state} ->
        if new_state.current_files && new_state.file_index < length(new_state.current_files) - 1 do
          # Move to next file
          case open_next_file(new_state) do
            {:ok, updated_state} ->
              read_batch_from_current_file(updated_state, batch_size, acc)
            {:error, _reason} ->
              # Return what we have so far
              if length(acc) > 0 do
                {{:ok, Enum.reverse(acc)}, new_state}
              else
                {:eof, new_state}
              end
          end
        else
          # No more files or single file mode
          if length(acc) > 0 do
            {{:ok, Enum.reverse(acc)}, new_state}
          else
            {:eof, new_state}
          end
        end

      {:error, reason, new_state} ->
        Logger.error("Error reading from file: #{inspect(reason)}")
        if length(acc) > 0 do
          {{:ok, Enum.reverse(acc)}, new_state}
        else
          {{:error, reason}, new_state}
        end
    end
  end

  defp read_single_item(state) do
    case state.format do
      :lines -> read_line_item(state)
      :json -> read_json_item(state)
      :csv -> read_csv_item(state)
      :custom -> read_custom_item(state)
    end
  end

  defp read_line_item(state) do
    case IO.read(state.file_handle, :line) do
      :eof ->
        {:eof, state}

      {:error, reason} ->
        {:error, reason, state}

      line when is_binary(line) ->
        # Remove trailing newline
        cleaned_line = String.trim_trailing(line, "\n\r")
        
        message = Message.new(state.topic, cleaned_line, 
          key: "line_#{state.line_number}",
          metadata: %{
            file_path: state.file_path,
            line_number: state.line_number,
            source_type: "file_line"
          }
        )

        new_state = %{state | 
          line_number: state.line_number + 1,
          bytes_read: state.bytes_read + byte_size(line)
        }

        {:ok, message, new_state}
    end
  end

  defp read_json_item(state) do
    case IO.read(state.file_handle, :line) do
      :eof ->
        {:eof, state}

      {:error, reason} ->
        {:error, reason, state}

      line when is_binary(line) ->
        cleaned_line = String.trim(line)
        
        if cleaned_line == "" do
          # Skip empty lines
          read_json_item(state)
        else
          case Jason.decode(cleaned_line) do
            {:ok, json_data} ->
              message = Message.new(state.topic, json_data,
                key: "json_#{state.line_number}",
                metadata: %{
                  file_path: state.file_path,
                  line_number: state.line_number,
                  source_type: "file_json"
                }
              )

              new_state = %{state | 
                line_number: state.line_number + 1,
                bytes_read: state.bytes_read + byte_size(line)
              }

              {:ok, message, new_state}

            {:error, reason} ->
              Logger.warning("Invalid JSON at line #{state.line_number}: #{inspect(reason)}")
              # Skip invalid JSON and continue
              new_state = %{state | line_number: state.line_number + 1}
              read_json_item(new_state)
          end
        end
    end
  end

  defp read_csv_item(state) do
    case IO.read(state.file_handle, :line) do
      :eof ->
        {:eof, state}

      {:error, reason} ->
        {:error, reason, state}

      line when is_binary(line) ->
        cleaned_line = String.trim_trailing(line, "\n\r")
        
        # Simple CSV parsing (can be enhanced with proper CSV library)
        fields = String.split(cleaned_line, ",")
        |> Enum.map(&String.trim/1)
        |> Enum.map(fn field ->
          # Remove quotes if present
          if String.starts_with?(field, "\"") && String.ends_with?(field, "\"") do
            String.slice(field, 1..-2//1)
          else
            field
          end
        end)

        csv_data = %{
          fields: fields,
          raw_line: cleaned_line
        }

        message = Message.new(state.topic, csv_data,
          key: "csv_#{state.line_number}",
          metadata: %{
            file_path: state.file_path,
            line_number: state.line_number,
            source_type: "file_csv"
          }
        )

        new_state = %{state | 
          line_number: state.line_number + 1,
          bytes_read: state.bytes_read + byte_size(line)
        }

        {:ok, message, new_state}
    end
  end

  defp read_custom_item(state) do
    if state.parser && is_function(state.parser, 2) do
      case IO.read(state.file_handle, :line) do
        :eof ->
          {:eof, state}

        {:error, reason} ->
          {:error, reason, state}

        line when is_binary(line) ->
          try do
            case state.parser.(line, state) do
              {:ok, parsed_data} ->
                message = Message.new(state.topic, parsed_data,
                  key: "custom_#{state.line_number}",
                  metadata: %{
                    file_path: state.file_path,
                    line_number: state.line_number,
                    source_type: "file_custom"
                  }
                )

                new_state = %{state | 
                  line_number: state.line_number + 1,
                  bytes_read: state.bytes_read + byte_size(line)
                }

                {:ok, message, new_state}

              :skip ->
                # Parser wants to skip this line
                new_state = %{state | line_number: state.line_number + 1}
                read_custom_item(new_state)

              {:error, reason} ->
                Logger.warning("Parser error at line #{state.line_number}: #{inspect(reason)}")
                new_state = %{state | line_number: state.line_number + 1}
                read_custom_item(new_state)
            end
          rescue
            error ->
              Logger.error("Parser exception at line #{state.line_number}: #{inspect(error)}")
              new_state = %{state | line_number: state.line_number + 1}
              read_custom_item(new_state)
          end
      end
    else
      {:error, :no_parser_function, state}
    end
  end

  defp open_next_file(state) do
    # Close current file if open
    if state.file_handle do
      File.close(state.file_handle)
    end

    # Open next file
    next_index = state.file_index + 1
    
    if next_index < length(state.current_files) do
      next_file = Enum.at(state.current_files, next_index)
      
      case File.open(next_file, [:read, state.metadata[:encoding] || :utf8]) do
        {:ok, handle} ->
          new_state = %{state |
            file_handle: handle,
            file_path: next_file,
            file_index: next_index,
            line_number: 0,
            bytes_read: 0
          }
          
          Logger.info("FileSource opened next file: #{next_file}")
          {:ok, new_state}

        {:error, reason} ->
          Logger.error("Failed to open file #{next_file}: #{inspect(reason)}")
          {:error, reason}
      end
    else
      # No more files
      {:error, :no_more_files}
    end
  end

  defp skip_initial_lines(_handle, 0), do: :ok
  defp skip_initial_lines(handle, count) when count > 0 do
    case IO.read(handle, :line) do
      :eof -> :ok
      {:error, _} -> :ok
      _ -> skip_initial_lines(handle, count - 1)
    end
  end
end