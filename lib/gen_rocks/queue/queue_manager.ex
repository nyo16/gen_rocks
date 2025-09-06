defmodule GenRocks.Queue.QueueManager do
  @moduledoc """
  GenStage producer that manages a single partition queue with pluggable storage.
  Handles message storage, retrieval, and consumer coordination using storage adapters.
  """

  use GenStage
  require Logger

  alias GenRocks.Queue.Message
  alias GenRocks.Storage.Adapter

  defstruct [
    :topic,
    :partition,
    :storage,
    :offset_counter,
    :consumer_offsets,
    :config,
    :pending_messages
  ]

  @doc """
  Starts a queue manager for a specific topic and partition.
  """
  def start_link(topic, partition, opts \\ []) do
    GenStage.start_link(__MODULE__, {topic, partition, opts}, 
      name: via_tuple(topic, partition))
  end

  @doc """
  Enqueues a single message to the partition queue.
  """
  def enqueue(topic, partition, message) do
    GenStage.cast(via_tuple(topic, partition), {:enqueue, message})
  end

  @doc """
  Enqueues a batch of messages to the partition queue.
  """
  def enqueue_batch(topic, partition, messages) do
    GenStage.cast(via_tuple(topic, partition), {:enqueue_batch, messages})
  end

  @doc """
  Gets the current queue size for the partition.
  """
  def queue_size(topic, partition) do
    GenStage.call(via_tuple(topic, partition), :queue_size)
  end

  @doc """
  Gets consumer offset information.
  """
  def get_consumer_offset(topic, partition, consumer_group) do
    GenStage.call(via_tuple(topic, partition), {:get_consumer_offset, consumer_group})
  end

  @doc """
  Commits a consumer offset.
  """
  def commit_offset(topic, partition, consumer_group, offset) do
    GenStage.cast(via_tuple(topic, partition), {:commit_offset, consumer_group, offset})
  end

  @impl true
  def init({topic, partition, opts}) do
    config = %{
      storage_adapter: Keyword.get(opts, :storage_adapter, GenRocks.Storage.EtsAdapter),
      storage_config: Keyword.get(opts, :storage_config, %{}),
      batch_size: Keyword.get(opts, :batch_size, 100),
      max_queue_size: Keyword.get(opts, :max_queue_size, 10_000)
    }

    # Initialize storage adapter with appropriate configuration
    base_storage_config = case config.storage_adapter do
      GenRocks.Storage.EtsAdapter ->
        %{
          table_name: String.to_atom("queue_#{topic}_#{partition}")
        }
      GenRocks.Storage.DiskLogAdapter ->
        %{
          topic: topic,
          partition: partition,
          log_dir: "./gen_rocks_data"
        }
      _ ->
        # Generic configuration for unknown adapters
        %{
          topic: topic,
          partition: partition,
          path: Path.join(["data", topic, "partition_#{partition}"])
        }
    end
    
    storage_config = Map.merge(base_storage_config, config.storage_config)

    case Adapter.new(config.storage_adapter, storage_config) do
      {:ok, storage} ->
        # Load current offset and consumer offsets
        offset_counter = load_offset_counter(storage)
        consumer_offsets = load_consumer_offsets(storage)

        state = %__MODULE__{
          topic: topic,
          partition: partition,
          storage: storage,
          offset_counter: offset_counter,
          consumer_offsets: consumer_offsets,
          config: config,
          pending_messages: :queue.new()
        }

        Logger.info("QueueManager started for #{topic}:#{partition} with #{config.storage_adapter} at offset #{offset_counter}")
        {:producer, state}

      {:error, reason} ->
        Logger.error("Failed to initialize storage adapter: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl true
  def handle_demand(demand, state) when demand > 0 do
    # Get messages from pending queue first, then from disk if needed
    {messages, new_pending} = get_messages_for_demand(state, demand)
    
    new_state = %{state | pending_messages: new_pending}

    Logger.debug("QueueManager #{state.topic}:#{state.partition} fulfilling demand: #{demand}, sending: #{length(messages)}")
    
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_cast({:enqueue, message}, state) do
    new_state = store_message(message, state)
    {:noreply, [], new_state}
  end

  @impl true
  def handle_cast({:enqueue_batch, messages}, state) do
    new_state = Enum.reduce(messages, state, &store_message/2)
    Logger.debug("QueueManager #{state.topic}:#{state.partition} stored batch of #{length(messages)} messages")
    {:noreply, [], new_state}
  end

  @impl true
  def handle_cast({:commit_offset, consumer_group, offset}, state) do
    # Store consumer offset
    offset_key = "consumer_offset:#{consumer_group}"
    Adapter.call(state.storage, :put, [offset_key, "#{offset}", []])
    
    new_consumer_offsets = Map.put(state.consumer_offsets, consumer_group, offset)
    new_state = %{state | consumer_offsets: new_consumer_offsets}
    
    Logger.debug("QueueManager #{state.topic}:#{state.partition} committed offset #{offset} for group #{consumer_group}")
    {:noreply, [], new_state}
  end

  @impl true
  def handle_call(:queue_size, _from, state) do
    # Calculate queue size based on offset counter and minimum committed offset
    min_committed_offset = get_min_committed_offset(state.consumer_offsets)
    queue_size = max(0, state.offset_counter - min_committed_offset)
    {:reply, queue_size, [], state}
  end

  @impl true
  def handle_call({:get_consumer_offset, consumer_group}, _from, state) do
    offset = Map.get(state.consumer_offsets, consumer_group, 0)
    {:reply, offset, [], state}
  end

  @impl true
  def terminate(_reason, state) do
    if state.storage do
      Adapter.call(state.storage, :close, [])
    end
    Logger.info("QueueManager #{state.topic}:#{state.partition} terminated")
  end

  # Private functions

  defp store_message(message, state) do
    # Assign offset to message
    offset = state.offset_counter
    message_with_offset = %{message | offset: offset, partition: state.partition}
    
    # Store message in storage
    message_key = "msg:#{offset}"
    serialized_message = Message.serialize(message_with_offset)
    Adapter.call(state.storage, :put, [message_key, serialized_message, []])
    
    # Update offset counter
    new_offset = offset + 1
    Adapter.call(state.storage, :put, ["offset_counter", "#{new_offset}", []])
    
    # Add to pending messages queue
    new_pending = :queue.in(message_with_offset, state.pending_messages)
    
    %{state | 
      offset_counter: new_offset,
      pending_messages: new_pending
    }
  end

  defp get_messages_for_demand(state, demand) do
    get_messages_from_pending(state.pending_messages, demand, [])
  end

  defp get_messages_from_pending(queue, 0, acc) do
    {Enum.reverse(acc), queue}
  end
  
  defp get_messages_from_pending(queue, demand, acc) when demand > 0 do
    case :queue.out(queue) do
      {{:value, message}, new_queue} ->
        get_messages_from_pending(new_queue, demand - 1, [message | acc])
      {:empty, queue} ->
        # Could load more messages from disk here if needed
        {Enum.reverse(acc), queue}
    end
  end

  defp load_offset_counter(storage) do
    case Adapter.call(storage, :get, ["offset_counter", []]) do
      {:ok, value} -> String.to_integer(value)
      :not_found -> 0
    end
  end

  defp load_consumer_offsets(storage) do
    # Simple implementation - could be optimized with scan operations
    case Adapter.call(storage, :get, ["consumer_offsets", []]) do
      {:ok, value} -> 
        value
        |> Jason.decode!()
        |> Map.new(fn {k, v} -> {k, String.to_integer(v)} end)
      :not_found -> %{}
    end
  end

  defp get_min_committed_offset(consumer_offsets) when map_size(consumer_offsets) == 0, do: 0
  defp get_min_committed_offset(consumer_offsets) do
    consumer_offsets
    |> Map.values()
    |> Enum.min()
  end

  defp via_tuple(topic, partition) do
    {:via, Registry, {GenRocks.QueueRegistry, {topic, partition}}}
  end
end