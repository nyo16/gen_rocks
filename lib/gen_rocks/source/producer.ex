defmodule GenRocks.Source.Producer do
  @moduledoc """
  GenStage producer that wraps a source implementation.
  Automatically reads from the source and produces messages for downstream consumers.
  """

  use GenStage
  require Logger

  alias GenRocks.Queue.Message

  defstruct [
    :source_module,
    :source_state,
    :batch_size,
    :total_messages,
    :config,
    :buffer
  ]

  @doc """
  Starts the source producer.
  """
  def start_link(source_module, config, opts \\ []) do
    name = Keyword.get(opts, :name)
    GenStage.start_link(__MODULE__, {source_module, config, opts}, name: name)
  end

  @impl true
  def init({source_module, config, opts}) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    
    case source_module.init(config) do
      {:ok, source_state} ->
        state = %__MODULE__{
          source_module: source_module,
          source_state: source_state,
          batch_size: batch_size,
          total_messages: 0,
          config: config,
          buffer: :queue.new()
        }

        Logger.info("Source producer started with #{source_module}")
        {:producer, state}

      {:error, reason} ->
        Logger.error("Failed to initialize source #{source_module}: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl true
  def handle_demand(demand, state) when demand > 0 do
    {messages, new_state} = get_messages_for_demand(state, demand, [])
    
    Logger.debug("Source producer fulfilling demand: #{demand}, sending: #{length(messages)}")
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_info(:read_more, state) do
    # Periodic reading trigger
    schedule_read_more(5000)
    {:noreply, [], state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Source producer received unknown message: #{inspect(msg)}")
    {:noreply, [], state}
  end

  @impl true
  def terminate(_reason, state) do
    if state.source_state do
      state.source_module.close(state.source_state)
    end
    Logger.info("Source producer terminated")
  end

  # Private functions

  defp get_messages_for_demand(state, demand, acc) when demand <= 0 do
    {Enum.reverse(acc), state}
  end

  defp get_messages_for_demand(state, demand, acc) do
    # First, try to get messages from buffer
    case dequeue_from_buffer(state.buffer, demand, acc) do
      {messages, new_buffer, 0} ->
        # Buffer satisfied the full demand
        new_state = %{state | buffer: new_buffer}
        {Enum.reverse(messages), new_state}

      {messages, new_buffer, remaining_demand} ->
        # Need to read more from source
        new_state = %{state | buffer: new_buffer}
        
        case read_from_source(new_state) do
          {:ok, source_messages, updated_state} ->
            # Add new messages to buffer and continue
            updated_buffer = Enum.reduce(source_messages, updated_state.buffer, fn msg, buf ->
              :queue.in(msg, buf)
            end)
            
            final_state = %{updated_state | buffer: updated_buffer}
            get_messages_for_demand(final_state, remaining_demand, messages)

          {:eof, updated_state} ->
            # No more messages available
            {Enum.reverse(messages), updated_state}

          {:error, _reason, updated_state} ->
            # Error reading from source, return what we have
            {Enum.reverse(messages), updated_state}
        end
    end
  end

  defp dequeue_from_buffer(buffer, demand, acc) when demand <= 0 do
    {acc, buffer, 0}
  end

  defp dequeue_from_buffer(buffer, demand, acc) do
    case :queue.out(buffer) do
      {{:value, message}, new_buffer} ->
        dequeue_from_buffer(new_buffer, demand - 1, [message | acc])
      {:empty, buffer} ->
        {acc, buffer, demand}
    end
  end

  defp read_from_source(state) do
    case state.source_module.read_batch(state.source_state, state.batch_size) do
      {:eof, new_source_state} ->
        {:eof, %{state | source_state: new_source_state}}

      {{:ok, messages}, new_source_state} when is_list(messages) ->
        new_total = state.total_messages + length(messages)
        new_state = %{state | 
          source_state: new_source_state,
          total_messages: new_total
        }
        {:ok, messages, new_state}

      {{:error, reason}, new_source_state} ->
        Logger.error("Source read error: #{inspect(reason)}")
        new_state = %{state | source_state: new_source_state}
        {:error, reason, new_state}
    end
  end

  defp schedule_read_more(delay_ms) do
    Process.send_after(self(), :read_more, delay_ms)
  end
end