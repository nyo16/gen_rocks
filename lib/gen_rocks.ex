defmodule GenRocks do
  @moduledoc """
  GenRocks - A distributed queueing system built with Elixir GenStage.
  
  Provides Apache Beam-like PCollection functionality with Flow transformations,
  Kafka-like distributed queuing with pluggable storage adapters (ETS, RocksDB),
  and comprehensive Sources/Sinks for data integration.

  ## Features

  - **Distributed Queuing**: Topic-based message queuing with partitioning
  - **Storage Adapters**: Pluggable storage (ETS for development, RocksDB for production)
  - **GenStage Integration**: Built on GenStage for back-pressure and flow control
  - **Flow Processing**: Apache Beam-style PCollection transformations
  - **Consumer Groups**: Load-balanced message consumption
  - **Sources & Sinks**: File-based data ingestion and output (extensible)
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

      # File-based data processing
      GenRocks.read_file("input.csv", format: :csv)
      |> GenRocks.transform(&process_row/1)
      |> GenRocks.write_file("output.json", format: :json)
      |> Flow.run()

  ## Architecture

      [Sources] -> [Producers] -> [Routers] -> [Queue Managers] -> [Consumer Groups] -> [Sinks]
                        |                           |                      |
                   [Flow Pipeline]           [Storage Adapters]      [Flow Pipeline]

  ## Components

  - **Sources**: File, Database, API data ingestion (extensible)
  - **Storage Adapters**: ETS (development), RocksDB (production), custom adapters
  - **Sinks**: File, Database, API data output (extensible)
  - **Flow Processing**: Apache Beam PCollection-equivalent transformations
  """

  alias GenRocks.Queue.{Supervisor, TopicProducer, ConsumerGroup, FlowProcessor}
  alias GenRocks.{Source, Sink}

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

  # Sources and Sinks

  @doc """
  Creates a Flow from a data source.
  Similar to Apache Beam's Read transform.

  ## Examples

      # Read from a text file
      source_config = %{
        file_path: "data/input.txt",
        format: :lines,
        topic: "file_data"
      }

      result = 
        GenRocks.from_source(GenRocks.Source.FileSource, source_config)
        |> GenRocks.filter(fn msg -> String.length(msg.value) > 10 end)
        |> GenRocks.collect()

      # Read JSON lines
      json_config = %{
        file_path: "data/events.jsonl",
        format: :json,
        topic: "events"
      }

      GenRocks.from_source(GenRocks.Source.FileSource, json_config)
      |> GenRocks.transform(fn msg -> %{msg | processed: true} end)
      |> GenRocks.run_to_sink(GenRocks.Sink.FileSink, %{
        file_path: "output/processed.jsonl",
        format: :json
      })
  """
  def from_source(source_module, config, opts \\ []) do
    Source.to_flow(source_module, config, opts)
  end

  @doc """
  Starts a source producer that feeds data into a topic.

  ## Examples

      # Start file source that feeds into topic
      {:ok, _} = GenRocks.start_source_producer(
        GenRocks.Source.FileSource,
        %{file_path: "logs/app.log", format: :lines, topic: "logs"},
        name: :log_producer
      )

      # Then consume from the topic
      {:ok, _} = GenRocks.start_consumer_group("processors", "logs", consumer_fn)
  """
  def start_source_producer(source_module, config, opts \\ []) do
    Source.start_producer(source_module, config, opts)
  end

  @doc """
  Writes Flow results to a data sink.
  Similar to Apache Beam's Write transform.

  ## Examples

      # Write to a file
      GenRocks.from_topic("events")
      |> GenRocks.transform(&process_event/1)
      |> GenRocks.to_sink(GenRocks.Sink.FileSink, %{
        file_path: "output/processed_events.jsonl",
        format: :json
      })

      # Write CSV with headers
      GenRocks.from_topic("users")
      |> GenRocks.to_sink(GenRocks.Sink.FileSink, %{
        file_path: "output/users.csv",
        format: :csv,
        csv_headers: ["id", "name", "email"]
      })
  """
  def to_sink(flow, sink_module, config, opts \\ []) do
    sink_fn = Sink.to_flow_sink(sink_module, config, opts)
    sink_fn.(flow)
  end

  @doc """
  Runs a Flow pipeline and writes results to a sink.
  Convenience function that combines collect and sink operations.

  ## Examples

      GenRocks.from_source(GenRocks.Source.FileSource, source_config)
      |> GenRocks.filter(&important_message?/1)
      |> GenRocks.run_to_sink(GenRocks.Sink.FileSink, sink_config)
  """
  def run_to_sink(flow, sink_module, config, opts \\ []) do
    flow
    |> to_sink(sink_module, config, opts)
    |> Flow.run()
  end

  @doc """
  Starts a sink consumer that writes messages from a topic to an external system.

  ## Examples

      # Start sink consumer that writes topic data to file
      {:ok, _} = GenRocks.start_sink_consumer(
        GenRocks.Sink.FileSink,
        %{file_path: "archive/events.jsonl", format: :json},
        subscribe_to: [{:via, Registry, {GenRocks.ProducerRegistry, "events"}}],
        name: :file_writer
      )
  """
  def start_sink_consumer(sink_module, config, opts \\ []) do
    Sink.start_consumer(sink_module, config, opts)
  end

  # File-based convenience functions

  @doc """
  Convenience function to read from a file and create a Flow.

  ## Examples

      # Read text file line by line
      GenRocks.read_file("input.txt")
      |> GenRocks.filter(fn msg -> not String.starts_with?(msg.value, "#") end)
      |> GenRocks.collect()

      # Read JSON lines file
      GenRocks.read_file("events.jsonl", format: :json)
      |> GenRocks.transform(&enrich_event/1)
      |> GenRocks.collect()
  """
  def read_file(file_path, opts \\ []) do
    format = Keyword.get(opts, :format, :lines)
    topic = Keyword.get(opts, :topic, "file_data")
    
    config = %{
      file_path: file_path,
      format: format,
      topic: topic
    }

    from_source(GenRocks.Source.FileSource, config, opts)
  end

  @doc """
  Convenience function to write Flow results to a file.

  ## Examples

      result
      |> GenRocks.write_file("output.txt")

      processed_data
      |> GenRocks.write_file("results.json", format: :json)

      user_data  
      |> GenRocks.write_file("users.csv", 
        format: :csv, 
        csv_headers: ["id", "name", "email"]
      )
  """
  def write_file(flow, file_path, opts \\ []) do
    format = Keyword.get(opts, :format, :lines)
    
    config = Map.new(opts)
    |> Map.put(:file_path, file_path)
    |> Map.put(:format, format)

    to_sink(flow, GenRocks.Sink.FileSink, config, opts)
  end
end