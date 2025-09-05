defmodule GenRocks do
  @moduledoc """
  GenRocks - A distributed queueing system built with Elixir GenStage.
  
  Provides Apache Beam-like PCollection functionality with Flow transformations,
  Kafka-like distributed queuing with pluggable storage adapters (ETS, RocksDB).

  ## Features

  - **Distributed Queuing**: Topic-based message queuing with partitioning
  - **Storage Adapters**: Pluggable storage (ETS for development, RocksDB for production)
  - **GenStage Integration**: Built on GenStage for back-pressure and flow control
  - **Flow Processing**: Apache Beam-style PCollection transformations
  - **Consumer Groups**: Load-balanced message consumption
  - **Fault Tolerance**: Supervision trees and error handling

  ## Quick Start

      # Start a topic with partitions
      {:ok, _} = GenRocks.start_topic("events", 4)

      # Produce messages
      GenRocks.publish("events", %{user_id: 123, action: "login"})

      # Start consumer group
      consumer_fn = fn message, _context ->
        IO.inspect(message.value)
        :ok
      end

      {:ok, _} = GenRocks.start_consumer_group("processors", "events", consumer_fn)

      # Flow processing (Apache Beam style)
      result = 
        GenRocks.from_topic("events")
        |> GenRocks.filter(fn msg -> msg.value.action == "purchase" end)
        |> GenRocks.transform(fn msg -> %{msg | processed: true} end)
        |> GenRocks.collect()

  ## Architecture

      [Producer] -> [Partition Router] -> [Queue Managers] -> [Consumer Groups]
                                              |
                                         [Flow Pipeline]

  ## Storage Adapters

  - **ETS**: In-memory storage for development and testing
  - **RocksDB**: Persistent storage for production use
  - **Custom**: Implement your own storage adapter
  """

  alias GenRocks.Queue.{Supervisor, TopicProducer, ConsumerGroup, FlowProcessor}

  # Topic Management

  @doc """
  Starts a new topic with the specified number of partitions.

  ## Options

  - `:storage_adapter` - Storage adapter module (default: `GenRocks.Storage.EtsAdapter`)
  - `:storage_config` - Configuration for the storage adapter
  - `:routing_strategy` - Message routing strategy (`:consistent_hash`, `:round_robin`, `:key_hash`)

  ## Examples

      # Start topic with ETS storage
      {:ok, topic} = GenRocks.start_topic("events", 4)

      # Start topic with RocksDB storage
      {:ok, topic} = GenRocks.start_topic("logs", 8, 
        storage_adapter: GenRocks.Storage.RocksdbAdapter,
        storage_config: %{path: "data/logs"}
      )
  """
  def start_topic(topic, partition_count, opts \\ []) do
    Supervisor.start_topic(topic, partition_count, opts)
  end

  @doc """
  Stops a topic and all its associated processes.
  """
  def stop_topic(topic) do
    Supervisor.stop_topic(topic)
  end

  @doc """
  Lists all running topics.
  """
  def list_topics do
    Supervisor.list_topics()
  end

  # Message Production

  @doc """
  Publishes a message to a topic.

  ## Options

  - `:key` - Partition key for message routing
  - `:headers` - Message headers
  - `:timestamp` - Message timestamp (defaults to current time)
  - `:metadata` - Additional metadata

  ## Examples

      # Simple message
      GenRocks.publish("events", %{user_id: 123, action: "login"})

      # With routing key
      GenRocks.publish("events", %{data: "value"}, key: "user_123")

      # With headers and metadata
      GenRocks.publish("events", %{data: "value"}, 
        key: "user_123",
        headers: %{"content-type" => "application/json"},
        metadata: %{source: "web_app"}
      )
  """
  def publish(topic, value, opts \\ []) do
    TopicProducer.publish(topic, value, opts)
  end

  @doc """
  Publishes a batch of messages to a topic.
  """
  def publish_batch(topic, messages) do
    TopicProducer.publish_batch(topic, messages)
  end

  # Consumer Groups

  @doc """
  Starts a consumer group for a topic.

  The consumer function receives a message and context, and should return:
  - `:ok` - Message processed successfully
  - `{:ok, result}` - Message processed with result
  - `{:error, reason}` - Message processing failed

  ## Options

  - `:partitions` - List of partitions to consume (default: all)
  - `:max_demand` - Maximum demand for back-pressure (default: 10)
  - `:auto_commit` - Automatically commit offsets (default: true)
  - `:commit_interval` - Offset commit interval in ms (default: 5000)

  ## Examples

      # Basic consumer
      consumer_fn = fn message, context ->
        IO.inspect(message.value)
        :ok
      end

      {:ok, _} = GenRocks.start_consumer_group("group1", "events", consumer_fn)

      # Consumer with error handling
      consumer_fn = fn message, context ->
        case process_message(message) do
          :ok -> :ok
          error -> {:error, error}
        end
      end

      {:ok, _} = GenRocks.start_consumer_group("processors", "events", consumer_fn,
        partitions: [0, 1],
        max_demand: 50,
        auto_commit: false
      )
  """
  def start_consumer_group(group_id, topic, consumer_function, opts \\ []) do
    Supervisor.start_consumer_group(group_id, topic, consumer_function, opts)
  end

  @doc """
  Stops a consumer group.
  """
  def stop_consumer_group(group_id, topic) do
    Supervisor.stop_consumer_group(group_id, topic)
  end

  @doc """
  Lists all running consumer groups.
  """
  def list_consumer_groups do
    Supervisor.list_consumer_groups()
  end

  # Flow Processing (Apache Beam style)

  @doc """
  Creates a Flow processing pipeline from a topic.
  Similar to creating a PCollection in Apache Beam.

  ## Options

  - `:stages` - Number of parallel stages (default: number of schedulers)
  - `:partition_count` - Number of partitions to read from
  - `:max_demand` - Maximum demand per partition

  ## Examples

      # Basic flow from topic
      result = 
        GenRocks.from_topic("events")
        |> GenRocks.filter(fn msg -> msg.value.important end)
        |> GenRocks.collect()

      # High-performance flow
      result = 
        GenRocks.from_topic("logs", stages: 8, partition_count: 16)
        |> GenRocks.transform(&process_log/1)
        |> GenRocks.collect()
  """
  def from_topic(topic, opts \\ []) do
    FlowProcessor.from_topic(topic, opts)
  end

  @doc """
  Creates a Flow from an enumerable of messages.
  """
  def from_enumerable(messages, opts \\ []) do
    FlowProcessor.from_enumerable(messages, opts)
  end

  @doc """
  Applies a transformation function to each message.
  Similar to Apache Beam's ParDo.

      GenRocks.from_topic("events")
      |> GenRocks.transform(fn msg -> 
        %{msg | value: String.upcase(msg.value)}
      end)
  """
  def transform(flow, transform_fn) do
    FlowProcessor.transform(flow, transform_fn)
  end

  @doc """
  Filters messages based on a predicate.

      GenRocks.from_topic("events")
      |> GenRocks.filter(fn msg -> msg.value.level == "ERROR" end)
  """
  def filter(flow, predicate_fn) do
    FlowProcessor.filter(flow, predicate_fn)
  end

  @doc """
  Groups messages by a key function.
  Similar to Apache Beam's GroupByKey.

      GenRocks.from_topic("events")
      |> GenRocks.group_by_key(fn msg -> msg.value.user_id end)
  """
  def group_by_key(flow, key_fn, opts \\ []) do
    FlowProcessor.group_by_key(flow, key_fn, opts)
  end

  @doc """
  Reduces messages using an accumulator function.
  Similar to Apache Beam's Combine.

      GenRocks.from_topic("metrics")
      |> GenRocks.reduce(
        fn -> %{sum: 0, count: 0} end,
        fn msg, acc -> 
          %{sum: acc.sum + msg.value, count: acc.count + 1}
        end
      )
  """
  def reduce(flow, initial_acc_fn, reducer_fn) do
    FlowProcessor.reduce(flow, initial_acc_fn, reducer_fn)
  end

  @doc """
  Aggregates messages using common aggregation functions.

      # Count messages
      GenRocks.from_topic("events")
      |> GenRocks.aggregate(:count)

      # Sum values
      GenRocks.from_topic("metrics")
      |> GenRocks.aggregate(:sum, value_fn: fn msg -> msg.value.amount end)

      # Average
      GenRocks.from_topic("metrics")
      |> GenRocks.aggregate(:average, value_fn: fn msg -> msg.value.response_time end)
  """
  def aggregate(flow, aggregation_type, opts \\ []) do
    FlowProcessor.aggregate(flow, aggregation_type, opts)
  end

  @doc """
  Applies windowing for time-based processing.

      # Fixed 5-second windows
      GenRocks.from_topic("events")
      |> GenRocks.window({:fixed, 5000})

      # Sliding windows
      GenRocks.from_topic("events")
      |> GenRocks.window({:sliding, 10000, 1000})
  """
  def window(flow, window_spec) do
    FlowProcessor.window(flow, window_spec)
  end

  @doc """
  Applies side effects without modifying the flow.
  Useful for logging, metrics, or external API calls.

      GenRocks.from_topic("events")
      |> GenRocks.side_effect(fn msg -> 
        Logger.info("Processing: #{inspect(msg)}")
      end)
  """
  def side_effect(flow, side_effect_fn) do
    FlowProcessor.side_effect(flow, side_effect_fn)
  end

  @doc """
  Materializes the flow into a concrete result.
  """
  def collect(flow) do
    FlowProcessor.collect(flow)
  end

  @doc """
  Runs the flow and sends results to a destination.

      # Send to a function
      GenRocks.from_topic("events")
      |> GenRocks.run_to(fn result -> IO.inspect(result) end)

      # Send to a GenStage process
      GenRocks.from_topic("events")
      |> GenRocks.run_to(consumer_pid)
  """
  def run_to(flow, destination) do
    FlowProcessor.run_to(flow, destination)
  end

  # System Information

  @doc """
  Gets system status and statistics.
  """
  def system_status do
    Supervisor.get_system_status()
  end

  @doc """
  Gets statistics for a consumer group.
  """
  def consumer_group_stats(group_id, topic) do
    ConsumerGroup.get_stats(group_id, topic)
  end
end