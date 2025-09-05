defmodule GenRocks.Queue.PartitionRouter do
  @moduledoc """
  GenStage producer-consumer that routes messages to specific partitions.
  Implements consistent hashing for message distribution across queue managers.
  """

  use GenStage
  require Logger

  alias GenRocks.Queue.{Message, QueueManager}

  defstruct [
    :topic,
    :partitions,
    :partition_count,
    :hash_ring,
    :routing_strategy
  ]

  @doc """
  Starts the partition router for a given topic.
  """
  def start_link(topic, partition_count, opts \\ []) do
    GenStage.start_link(__MODULE__, {topic, partition_count, opts}, 
      name: via_tuple(topic))
  end

  @impl true
  def init({topic, partition_count, opts}) do
    routing_strategy = Keyword.get(opts, :routing_strategy, :consistent_hash)
    
    # Initialize partitions
    partitions = 0..(partition_count - 1)
                |> Enum.map(&{&1, []})
                |> Map.new()

    # Create hash ring for consistent hashing
    hash_ring = create_hash_ring(partition_count)

    state = %__MODULE__{
      topic: topic,
      partitions: partitions,
      partition_count: partition_count,
      hash_ring: hash_ring,
      routing_strategy: routing_strategy
    }

    # Start queue managers for each partition
    start_queue_managers(topic, partition_count)

    Logger.info("PartitionRouter started for topic: #{topic} with #{partition_count} partitions")
    {:producer_consumer, state}
  end

  @impl true
  def handle_events(messages, _from, state) do
    # Route messages to partitions
    {routed_messages, stats} = route_messages(messages, state)
    
    # Send messages to appropriate queue managers
    Enum.each(routed_messages, fn {partition, partition_messages} ->
      QueueManager.enqueue_batch(state.topic, partition, partition_messages)
    end)

    # Log routing statistics
    if map_size(stats) > 0 do
      Logger.debug("PartitionRouter #{state.topic} routed messages: #{inspect(stats)}")
    end

    # Forward all messages downstream (for monitoring/transformation stages)
    {:noreply, messages, state}
  end

  @doc """
  Gets partition assignment for a given message key.
  """
  def get_partition(topic, key) do
    GenStage.call(via_tuple(topic), {:get_partition, key})
  end

  @impl true
  def handle_call({:get_partition, key}, _from, state) do
    partition = calculate_partition(key, state)
    {:reply, partition, [], state}
  end

  @doc """
  Gets current partition statistics.
  """
  def get_partition_stats(topic) do
    GenStage.call(via_tuple(topic), :get_partition_stats)
  end

  @impl true
  def handle_call(:get_partition_stats, _from, state) do
    stats = Enum.map(0..(state.partition_count - 1), fn partition ->
      queue_size = QueueManager.queue_size(state.topic, partition)
      {partition, queue_size}
    end)
    |> Map.new()

    {:reply, stats, [], state}
  end

  # Private functions

  defp route_messages(messages, state) do
    messages
    |> Enum.reduce({%{}, %{}}, fn message, {routed_acc, stats_acc} ->
      partition = calculate_partition(Message.partition_key(message), state)
      
      # Add message to partition group
      partition_messages = Map.get(routed_acc, partition, [])
      new_routed = Map.put(routed_acc, partition, [message | partition_messages])
      
      # Update statistics
      partition_count = Map.get(stats_acc, partition, 0)
      new_stats = Map.put(stats_acc, partition, partition_count + 1)
      
      {new_routed, new_stats}
    end)
    |> then(fn {routed, stats} ->
      # Reverse message lists to maintain order
      final_routed = Enum.map(routed, fn {partition, messages} ->
        {partition, Enum.reverse(messages)}
      end)
      {final_routed, stats}
    end)
  end

  defp calculate_partition(key, state) do
    case state.routing_strategy do
      :consistent_hash ->
        hash = :erlang.phash2(key)
        find_partition_in_ring(hash, state.hash_ring)
      
      :round_robin ->
        # Simple round-robin based on hash
        :erlang.phash2(key) |> rem(state.partition_count)
      
      :key_hash ->
        # Direct hash modulo partition count
        :erlang.phash2(key) |> rem(state.partition_count)
    end
  end

  defp create_hash_ring(partition_count) do
    # Create virtual nodes for better distribution
    virtual_nodes_per_partition = 150
    
    0..(partition_count - 1)
    |> Enum.flat_map(fn partition ->
      0..(virtual_nodes_per_partition - 1)
      |> Enum.map(fn virtual ->
        hash = :erlang.phash2({partition, virtual})
        {hash, partition}
      end)
    end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp find_partition_in_ring(hash, ring) do
    ring
    |> Enum.find(fn {ring_hash, _partition} -> ring_hash >= hash end)
    |> case do
      {_ring_hash, partition} -> partition
      nil -> 
        # Wrap around to the first partition
        {_ring_hash, partition} = List.first(ring)
        partition
    end
  end

  defp start_queue_managers(topic, partition_count) do
    0..(partition_count - 1)
    |> Enum.each(fn partition ->
      QueueManager.start_link(topic, partition)
    end)
  end

  defp via_tuple(topic) do
    {:via, Registry, {GenRocks.RouterRegistry, topic}}
  end
end