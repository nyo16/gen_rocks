defmodule GenRocks.Source do
  @moduledoc """
  Behavior for data sources in the GenRocks system.
  Sources read data from external systems and feed it into the processing pipeline.
  
  Similar to Apache Beam's PTransform for reading data, sources provide
  the entry point for data into the distributed processing system.
  """

  @type source_config :: map()
  @type source_state :: term()
  @type read_result :: {:ok, [GenRocks.Queue.Message.t()]} | {:error, term()} | :eof

  @doc """
  Initializes the source with the given configuration.
  Returns initial state for the source.
  """
  @callback init(source_config()) :: {:ok, source_state()} | {:error, term()}

  @doc """
  Reads a batch of messages from the source.
  Should return messages wrapped in GenRocks.Queue.Message structs.
  """
  @callback read_batch(source_state(), batch_size :: pos_integer()) :: 
    {read_result(), source_state()}

  @doc """
  Closes the source and cleans up resources.
  """
  @callback close(source_state()) :: :ok

  @doc """
  Optional callback to get source metadata/statistics.
  """
  @callback info(source_state()) :: map()

  @optional_callbacks [info: 1]

  @doc """
  Starts a GenStage producer that reads from the given source.
  """
  def start_producer(source_module, config, opts \\ []) do
    GenRocks.Source.Producer.start_link(source_module, config, opts)
  end

  @doc """
  Creates a Flow from a source.
  """
  def to_flow(source_module, config, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    max_demand = Keyword.get(opts, :max_demand, 1000)
    
    Stream.resource(
      fn -> 
        case source_module.init(config) do
          {:ok, state} -> {source_module, state}
          {:error, reason} -> throw({:source_init_error, reason})
        end
      end,
      fn {module, state} ->
        case module.read_batch(state, batch_size) do
          {:eof, new_state} -> 
            {:halt, {module, new_state}}
          {{:ok, messages}, new_state} when is_list(messages) -> 
            {messages, {module, new_state}}
          {{:error, reason}, new_state} ->
            # Log error and continue
            require Logger
            Logger.error("Source read error: #{inspect(reason)}")
            {[], {module, new_state}}
        end
      end,
      fn {module, state} -> module.close(state) end
    )
    |> Flow.from_enumerable(max_demand: max_demand)
  end
end