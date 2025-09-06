defmodule GenRocks.Queue.ConsumerGroup do
  @moduledoc """
  GenStage consumer that manages a consumer group for distributed message consumption.
  Handles partition assignment, load balancing, and offset management.
  """

  use GenStage
  require Logger

  alias GenRocks.Queue.{Message, QueueManager}

  defstruct [
    :group_id,
    :topic,
    :subscriptions,
    :assigned_partitions,
    :consumer_function,
    :config,
    :stats
  ]

  @doc """
  Starts a consumer group for a given topic.
  """
  def start_link(group_id, topic, consumer_function, opts \\ []) do
    GenStage.start_link(__MODULE__, {group_id, topic, consumer_function, opts}, 
      name: via_tuple(group_id, topic))
  end

  @doc """
  Subscribes the consumer group to specific partitions.
  """
  def subscribe_to_partitions(group_id, topic, partitions) when is_list(partitions) do
    GenStage.cast(via_tuple(group_id, topic), {:subscribe_to_partitions, partitions})
  end

  @doc """
  Gets current consumer group statistics.
  """
  def get_stats(group_id, topic) do
    GenStage.call(via_tuple(group_id, topic), :get_stats)
  end

  @doc """
  Manually commits offsets for all assigned partitions.
  """
  def commit_offsets(group_id, topic) do
    GenStage.cast(via_tuple(group_id, topic), :commit_offsets)
  end

  @impl true
  def init({group_id, topic, consumer_function, opts}) do
    config = %{
      max_demand: Keyword.get(opts, :max_demand, 10),
      min_demand: Keyword.get(opts, :min_demand, 5),
      batch_size: Keyword.get(opts, :batch_size, 1),
      auto_commit: Keyword.get(opts, :auto_commit, true),
      commit_interval: Keyword.get(opts, :commit_interval, 5_000),
      partitions: Keyword.get(opts, :partitions, [])
    }

    # Schedule automatic offset commits if enabled
    if config.auto_commit do
      schedule_commit_offsets(config.commit_interval)
    end

    state = %__MODULE__{
      group_id: group_id,
      topic: topic,
      subscriptions: %{},
      assigned_partitions: MapSet.new(),
      consumer_function: consumer_function,
      config: config,
      stats: %{
        messages_processed: 0,
        last_commit_time: nil,
        processing_errors: 0
      }
    }

    # Auto-subscribe to configured partitions
    if not Enum.empty?(config.partitions) do
      send(self(), {:subscribe_to_partitions, config.partitions})
    end

    Logger.info("ConsumerGroup #{group_id} started for topic #{topic}")
    {:consumer, state}
  end

  @impl true
  def handle_events(messages, from, state) do
    partition = extract_partition_from_subscription(from, state)
    
    # Process messages and collect results
    {processed_count, errors, last_offset} = process_messages(messages, state)
    
    # Update statistics
    new_stats = %{state.stats |
      messages_processed: state.stats.messages_processed + processed_count,
      processing_errors: state.stats.processing_errors + errors
    }

    # Store last processed offset for this partition
    new_state = if last_offset do
      new_subscriptions = Map.put(state.subscriptions, partition, last_offset)
      %{state | subscriptions: new_subscriptions, stats: new_stats}
    else
      %{state | stats: new_stats}
    end

    Logger.debug("ConsumerGroup #{state.group_id} processed #{processed_count} messages from partition #{partition}")
    
    {:noreply, [], new_state}
  end

  @impl true
  def handle_cast({:subscribe_to_partitions, partitions}, state) do
    new_assigned = MapSet.union(state.assigned_partitions, MapSet.new(partitions))
    
    # Subscribe to queue managers for each partition
    Enum.each(partitions, fn partition ->
      if not MapSet.member?(state.assigned_partitions, partition) do
        subscribe_to_partition(state.topic, partition, state.config)
      end
    end)

    new_state = %{state | assigned_partitions: new_assigned}
    
    Logger.info("ConsumerGroup #{state.group_id} subscribed to partitions: #{inspect(partitions)}")
    {:noreply, [], new_state}
  end

  @impl true
  def handle_cast(:commit_offsets, state) do
    commit_result = commit_all_offsets(state)
    
    new_stats = %{state.stats | last_commit_time: DateTime.utc_now()}
    new_state = %{state | stats: new_stats}
    
    Logger.debug("ConsumerGroup #{state.group_id} committed offsets: #{inspect(commit_result)}")
    {:noreply, [], new_state}
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    detailed_stats = Map.merge(state.stats, %{
      group_id: state.group_id,
      topic: state.topic,
      assigned_partitions: MapSet.to_list(state.assigned_partitions),
      current_offsets: state.subscriptions
    })
    
    {:reply, detailed_stats, [], state}
  end

  @impl true
  def handle_info(:commit_offsets, state) do
    if state.config.auto_commit do
      commit_all_offsets(state)
      schedule_commit_offsets(state.config.commit_interval)
      
      new_stats = %{state.stats | last_commit_time: DateTime.utc_now()}
      new_state = %{state | stats: new_stats}
      
      {:noreply, [], new_state}
    else
      {:noreply, [], state}
    end
  end

  @impl true
  def handle_info({:subscribe_to_partitions, partitions}, state) do
    # Handle initial subscription from init/1
    handle_cast({:subscribe_to_partitions, partitions}, state)
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("ConsumerGroup received unknown message: #{inspect(msg)}")
    {:noreply, [], state}
  end

  # Private functions

  defp process_messages(messages, state) do
    Enum.reduce(messages, {0, 0, nil}, fn message, {processed, errors, _last_offset} ->
      try do
        # Call the user-provided consumer function
        result = state.consumer_function.(message, %{
          group_id: state.group_id,
          topic: state.topic,
          partition: message.partition
        })
        
        case result do
          :ok -> {processed + 1, errors, message.offset}
          {:ok, _} -> {processed + 1, errors, message.offset}
          {:error, reason} ->
            Logger.warning("Message processing failed: #{inspect(reason)}")
            {processed, errors + 1, message.offset}
          _ ->
            Logger.warning("Invalid consumer function return: #{inspect(result)}")
            {processed, errors + 1, message.offset}
        end
      rescue
        error ->
          Logger.error("Exception in consumer function: #{inspect(error)}")
          {processed, errors + 1, message.offset}
      end
    end)
  end

  defp extract_partition_from_subscription(_from, _state) do
    # In a real implementation, this would extract the partition from the subscription
    # For now, we'll use a simple approach
    0
  end

  defp subscribe_to_partition(topic, partition, config) do
    case QueueManager.start_link(topic, partition) do
      {:ok, _pid} ->
        subscription_opts = [
          max_demand: config.max_demand,
          min_demand: config.min_demand
        ]
        GenStage.sync_subscribe(self(), [
          to: {:via, Registry, {GenRocks.QueueRegistry, {topic, partition}}}
        ] ++ subscription_opts)
        
      {:error, {:already_started, _pid}} ->
        # Queue manager already exists, just subscribe
        subscription_opts = [
          max_demand: config.max_demand,
          min_demand: config.min_demand
        ]
        GenStage.sync_subscribe(self(), [
          to: {:via, Registry, {GenRocks.QueueRegistry, {topic, partition}}}
        ] ++ subscription_opts)
        
      error ->
        Logger.error("Failed to subscribe to partition #{partition}: #{inspect(error)}")
        error
    end
  end

  defp commit_all_offsets(state) do
    state.subscriptions
    |> Enum.map(fn {partition, offset} ->
      case QueueManager.commit_offset(state.topic, partition, state.group_id, offset) do
        :ok -> {:ok, partition, offset}
        error -> {:error, partition, offset, error}
      end
    end)
  end

  defp schedule_commit_offsets(interval) do
    Process.send_after(self(), :commit_offsets, interval)
  end

  defp via_tuple(group_id, topic) do
    {:via, Registry, {GenRocks.ConsumerRegistry, {group_id, topic}}}
  end
end