defmodule GenRocks.Queue.Supervisor do
  @moduledoc """
  Supervisor for the distributed queue system components.
  Manages topic producers, partition routers, and consumer groups.
  """

  use Supervisor
  require Logger

  @doc """
  Starts the queue supervisor.
  """
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Starts a new topic with the specified number of partitions.
  """
  def start_topic(topic, partition_count, opts \\ []) do
    # Start topic producer
    producer_spec = %{
      id: {:topic_producer, topic},
      start: {GenRocks.Queue.TopicProducer, :start_link, [topic, opts]},
      restart: :permanent,
      type: :worker
    }

    # Start partition router
    router_spec = %{
      id: {:partition_router, topic},
      start: {GenRocks.Queue.PartitionRouter, :start_link, [topic, partition_count, opts]},
      restart: :permanent,
      type: :worker
    }

    with {:ok, _} <- Supervisor.start_child(__MODULE__, producer_spec),
         {:ok, _} <- Supervisor.start_child(__MODULE__, router_spec) do
      
      # Subscribe router to producer
      GenStage.sync_subscribe(
        {:via, Registry, {GenRocks.RouterRegistry, topic}},
        to: {:via, Registry, {GenRocks.ProducerRegistry, topic}}
      )
      
      Logger.info("Topic #{topic} started with #{partition_count} partitions")
      {:ok, topic}
    else
      error ->
        Logger.error("Failed to start topic #{topic}: #{inspect(error)}")
        error
    end
  end

  @doc """
  Stops a topic and all its associated processes.
  """
  def stop_topic(topic) do
    # Stop topic producer
    case Supervisor.terminate_child(__MODULE__, {:topic_producer, topic}) do
      :ok -> Supervisor.delete_child(__MODULE__, {:topic_producer, topic})
      error -> Logger.warning("Failed to stop topic producer: #{inspect(error)}")
    end

    # Stop partition router
    case Supervisor.terminate_child(__MODULE__, {:partition_router, topic}) do
      :ok -> Supervisor.delete_child(__MODULE__, {:partition_router, topic})
      error -> Logger.warning("Failed to stop partition router: #{inspect(error)}")
    end

    Logger.info("Topic #{topic} stopped")
    :ok
  end

  @doc """
  Starts a consumer group for a topic.
  """
  def start_consumer_group(group_id, topic, consumer_function, opts \\ []) do
    spec = %{
      id: {:consumer_group, group_id, topic},
      start: {GenRocks.Queue.ConsumerGroup, :start_link, [group_id, topic, consumer_function, opts]},
      restart: :permanent,
      type: :worker
    }

    case Supervisor.start_child(__MODULE__, spec) do
      {:ok, pid} ->
        Logger.info("Consumer group #{group_id} started for topic #{topic}")
        {:ok, pid}
      error ->
        Logger.error("Failed to start consumer group #{group_id}: #{inspect(error)}")
        error
    end
  end

  @doc """
  Stops a consumer group.
  """
  def stop_consumer_group(group_id, topic) do
    case Supervisor.terminate_child(__MODULE__, {:consumer_group, group_id, topic}) do
      :ok -> 
        Supervisor.delete_child(__MODULE__, {:consumer_group, group_id, topic})
        Logger.info("Consumer group #{group_id} stopped")
        :ok
      error -> 
        Logger.warning("Failed to stop consumer group #{group_id}: #{inspect(error)}")
        error
    end
  end

  @doc """
  Lists all running topics.
  """
  def list_topics do
    __MODULE__
    |> Supervisor.which_children()
    |> Enum.filter(fn {id, _pid, _type, _modules} ->
      match?({:topic_producer, _topic}, id)
    end)
    |> Enum.map(fn {{:topic_producer, topic}, _pid, _type, _modules} -> topic end)
  end

  @doc """
  Lists all running consumer groups.
  """
  def list_consumer_groups do
    __MODULE__
    |> Supervisor.which_children()
    |> Enum.filter(fn {id, _pid, _type, _modules} ->
      match?({:consumer_group, _group_id, _topic}, id)
    end)
    |> Enum.map(fn {{:consumer_group, group_id, topic}, _pid, _type, _modules} -> 
      {group_id, topic}
    end)
  end

  @doc """
  Gets system status and statistics.
  """
  def get_system_status do
    children = Supervisor.which_children(__MODULE__)
    
    %{
      supervisor_pid: self(),
      children_count: length(children),
      topics: list_topics(),
      consumer_groups: list_consumer_groups(),
      uptime: get_uptime(),
      memory_usage: get_memory_usage()
    }
  end

  @impl true
  def init(opts) do
    # Configure storage adapter globally
    storage_adapter = Keyword.get(opts, :storage_adapter, GenRocks.Storage.EtsAdapter)
    storage_config = Keyword.get(opts, :storage_config, %{})
    
    Application.put_env(:gen_rocks, :storage_adapter, storage_adapter)
    Application.put_env(:gen_rocks, :storage_config, storage_config)

    # Start registries for process discovery
    children = [
      # Registry for topic producers
      {Registry, keys: :unique, name: GenRocks.ProducerRegistry},
      
      # Registry for partition routers
      {Registry, keys: :unique, name: GenRocks.RouterRegistry},
      
      # Registry for queue managers
      {Registry, keys: :unique, name: GenRocks.QueueRegistry},
      
      # Registry for consumer groups
      {Registry, keys: :unique, name: GenRocks.ConsumerRegistry}
    ]

    opts = [
      strategy: :one_for_one,
      name: __MODULE__
    ]

    Logger.info("GenRocks Queue Supervisor started with storage adapter: #{storage_adapter}")
    
    Supervisor.init(children, opts)
  end

  # Private helper functions

  defp get_uptime do
    {uptime_ms, _} = :erlang.statistics(:wall_clock)
    uptime_ms
  end

  defp get_memory_usage do
    :erlang.memory()
  end
end