defmodule GenRocks.Sink do
  @moduledoc """
  Behavior for data sinks in the GenRocks system.
  Sinks write processed data to external systems as the final stage of the pipeline.
  
  Similar to Apache Beam's PTransform for writing data, sinks provide
  the exit point for data from the distributed processing system.
  """

  @type sink_config :: map()
  @type sink_state :: term()
  @type write_result :: :ok | {:error, term()}

  @doc """
  Initializes the sink with the given configuration.
  Returns initial state for the sink.
  """
  @callback init(sink_config()) :: {:ok, sink_state()} | {:error, term()}

  @doc """
  Writes a batch of messages to the sink.
  Messages are GenRocks.Queue.Message structs.
  """
  @callback write_batch(sink_state(), [GenRocks.Queue.Message.t()]) :: 
    {write_result(), sink_state()}

  @doc """
  Flushes any buffered data to ensure durability.
  """
  @callback flush(sink_state()) :: {write_result(), sink_state()}

  @doc """
  Closes the sink and cleans up resources.
  """
  @callback close(sink_state()) :: :ok

  @doc """
  Optional callback to get sink metadata/statistics.
  """
  @callback info(sink_state()) :: map()

  @optional_callbacks [info: 1]

  @doc """
  Starts a GenStage consumer that writes to the given sink.
  """
  def start_consumer(sink_module, config, opts \\ []) do
    GenRocks.Sink.Consumer.start_link(sink_module, config, opts)
  end

  @doc """
  Creates a Flow sink that writes to the specified sink implementation.
  This can be used as the final stage in a Flow pipeline.
  """
  def to_flow_sink(sink_module, config, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    flush_interval = Keyword.get(opts, :flush_interval, 5000)
    
    fn flow ->
      flow
      |> Flow.map(fn message -> message end)  # Ensure messages are proper format
      |> Flow.partition(stages: 1)  # Single stage for ordered writing
      |> Flow.reduce(fn -> [] end, fn message, acc -> 
        [message | acc] 
      end)
      |> Flow.map(fn messages -> 
        # Write batch when partition completes
        case sink_module.init(config) do
          {:ok, state} ->
            reversed_messages = Enum.reverse(messages)
            case write_batch_with_retry(sink_module, state, reversed_messages, 3) do
              {:ok, final_state} -> 
                sink_module.close(final_state)
                {:ok, length(reversed_messages)}
              {:error, reason} -> 
                sink_module.close(state)
                {:error, reason}
            end
          {:error, reason} -> 
            {:error, reason}
        end
      end)
    end
  end

  @doc """
  Helper function to write messages with automatic retry logic.
  """
  def write_batch_with_retry(sink_module, state, messages, retries) when retries > 0 do
    case sink_module.write_batch(state, messages) do
      {:ok, new_state} -> 
        {:ok, new_state}
      {:error, reason} when retries > 1 -> 
        require Logger
        Logger.warning("Sink write failed, retrying: #{inspect(reason)}")
        Process.sleep(1000)  # Wait before retry
        write_batch_with_retry(sink_module, state, messages, retries - 1)
      {:error, reason} -> 
        {:error, reason}
    end
  end

  def write_batch_with_retry(_sink_module, state, _messages, 0) do
    {:error, :max_retries_exceeded}
  end
end