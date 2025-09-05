defmodule GenRocks.Sink.Consumer do
  @moduledoc """
  GenStage consumer that wraps a sink implementation.
  Automatically consumes messages from producers and writes them to the configured sink.
  """

  use GenStage
  require Logger

  defstruct [
    :sink_module,
    :sink_state,
    :batch_size,
    :flush_interval,
    :total_messages,
    :config,
    :buffer,
    :last_flush_time,
    :flush_timer
  ]

  @doc """
  Starts the sink consumer.
  """
  def start_link(sink_module, config, opts \\ []) do
    name = Keyword.get(opts, :name)
    GenStage.start_link(__MODULE__, {sink_module, config, opts}, name: name)
  end

  @impl true
  def init({sink_module, config, opts}) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    flush_interval = Keyword.get(opts, :flush_interval, 5000)
    max_demand = Keyword.get(opts, :max_demand, batch_size * 2)
    min_demand = Keyword.get(opts, :min_demand, div(batch_size, 2))

    case sink_module.init(config) do
      {:ok, sink_state} ->
        # Schedule periodic flush
        flush_timer = schedule_flush(flush_interval)

        state = %__MODULE__{
          sink_module: sink_module,
          sink_state: sink_state,
          batch_size: batch_size,
          flush_interval: flush_interval,
          total_messages: 0,
          config: config,
          buffer: [],
          last_flush_time: System.monotonic_time(:millisecond),
          flush_timer: flush_timer
        }

        Logger.info("Sink consumer started with #{sink_module}")
        {:consumer, state, subscribe_to: Keyword.get(opts, :subscribe_to, []), 
         max_demand: max_demand, min_demand: min_demand}

      {:error, reason} ->
        Logger.error("Failed to initialize sink #{sink_module}: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl true
  def handle_events(events, _from, state) do
    new_buffer = state.buffer ++ events
    new_total = state.total_messages + length(events)
    
    # Check if we should flush based on buffer size
    {final_buffer, new_sink_state} = 
      if length(new_buffer) >= state.batch_size do
        {batch_to_write, remaining_buffer} = Enum.split(new_buffer, state.batch_size)
        
        case write_batch_to_sink(state, batch_to_write) do
          {:ok, updated_sink_state} ->
            Logger.debug("Sink consumer wrote batch of #{length(batch_to_write)} messages")
            {remaining_buffer, updated_sink_state}
          {:error, reason} ->
            Logger.error("Failed to write batch to sink: #{inspect(reason)}")
            # Keep messages in buffer for retry
            {new_buffer, state.sink_state}
        end
      else
        {new_buffer, state.sink_state}
      end

    updated_state = %{state |
      buffer: final_buffer,
      total_messages: new_total,
      sink_state: new_sink_state
    }

    Logger.debug("Sink consumer processed #{length(events)} events, buffer size: #{length(final_buffer)}")
    {:noreply, [], updated_state}
  end

  @impl true
  def handle_info(:flush_buffer, state) do
    # Periodic flush
    {new_buffer, new_sink_state} = 
      if length(state.buffer) > 0 do
        case write_batch_to_sink(state, state.buffer) do
          {:ok, updated_sink_state} ->
            case state.sink_module.flush(updated_sink_state) do
              {:ok, flushed_state} ->
                Logger.debug("Sink consumer flushed #{length(state.buffer)} buffered messages")
                {[], flushed_state}
              {:error, reason} ->
                Logger.error("Failed to flush sink: #{inspect(reason)}")
                {state.buffer, updated_sink_state}
            end
          {:error, reason} ->
            Logger.error("Failed to write buffered messages: #{inspect(reason)}")
            {state.buffer, state.sink_state}
        end
      else
        # Flush even if no buffered messages (some sinks may need periodic flushing)
        case state.sink_module.flush(state.sink_state) do
          {:ok, flushed_state} -> {[], flushed_state}
          {:error, reason} ->
            Logger.error("Failed to flush sink: #{inspect(reason)}")
            {[], state.sink_state}
        end
      end

    # Schedule next flush
    new_flush_timer = schedule_flush(state.flush_interval)
    
    updated_state = %{state |
      buffer: new_buffer,
      sink_state: new_sink_state,
      last_flush_time: System.monotonic_time(:millisecond),
      flush_timer: new_flush_timer
    }

    {:noreply, [], updated_state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Sink consumer received unknown message: #{inspect(msg)}")
    {:noreply, [], state}
  end

  @impl true
  def terminate(reason, state) do
    Logger.info("Sink consumer terminating: #{inspect(reason)}")
    
    # Cancel flush timer
    if state.flush_timer do
      Process.cancel_timer(state.flush_timer)
    end

    # Write any remaining buffered messages
    if length(state.buffer) > 0 do
      case write_batch_to_sink(state, state.buffer) do
        {:ok, updated_state} ->
          Logger.info("Wrote #{length(state.buffer)} buffered messages on termination")
          state.sink_module.flush(updated_state)
        {:error, reason} ->
          Logger.error("Failed to write buffered messages on termination: #{inspect(reason)}")
      end
    end

    # Close the sink
    if state.sink_state do
      state.sink_module.close(state.sink_state)
    end
    
    Logger.info("Sink consumer terminated, total messages processed: #{state.total_messages}")
  end

  # Private functions

  defp write_batch_to_sink(state, messages) do
    case state.sink_module.write_batch(state.sink_state, messages) do
      {:ok, new_sink_state} ->
        {:ok, new_sink_state}
      {:error, reason} ->
        Logger.error("Sink write_batch failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp schedule_flush(interval_ms) do
    Process.send_after(self(), :flush_buffer, interval_ms)
  end
end