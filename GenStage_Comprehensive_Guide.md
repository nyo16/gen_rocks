# GenStage Comprehensive Guide: Building Distributed Queueing Systems in Elixir

## Overview

GenStage is a behavior for building data processing pipelines with back-pressure and flow control in Elixir. It provides a foundation for creating distributed queueing processing systems similar to Kafka, with native Elixir concurrency and fault tolerance capabilities.

## Core Concepts

### 1. Stage Types

GenStage supports three primary stage types:

#### Producer
- **Role**: Generates events and waits for demand from consumers
- **Key Function**: `handle_demand/2` - responds to consumer requests
- **Use Cases**: Data generators, database readers, message queue publishers

#### Consumer
- **Role**: Requests and processes events from producers
- **Key Function**: `handle_events/3` - processes incoming events
- **Use Cases**: Data sinks, processors that don't emit new events

#### Producer-Consumer
- **Role**: Both consumes events from upstream and produces events for downstream
- **Key Functions**: Both `handle_events/3` and can emit new events
- **Use Cases**: Data transformers, filters, mappers in processing pipelines

### 2. Back-Pressure Mechanism

GenStage employs a demand-driven flow control system:

- Consumers signal to producers how much data they can process
- Producers only emit events matching consumer demand
- Prevents overwhelming downstream stages
- Enables automatic load balancing across the pipeline

### 3. Subscription Model

Stages communicate through subscriptions:
- Consumers subscribe to producers
- Subscriptions carry demand information upstream
- Support for multiple subscription patterns
- Configurable cancellation behaviors (`:permanent`, `:transient`, `:temporary`)

## Implementation Patterns

### Basic Producer Example

```elixir
defmodule NumberProducer do
  use GenStage

  def start_link(initial) do
    GenStage.start_link(__MODULE__, initial)
  end

  def init(counter) do
    {:producer, counter}
  end

  def handle_demand(demand, counter) when demand > 0 do
    events = Enum.to_list(counter..counter+demand-1)
    {:noreply, events, counter + demand}
  end
end
```

### Basic Consumer Example

```elixir
defmodule PrinterConsumer do
  use GenStage

  def start_link() do
    GenStage.start_link(__MODULE__, :ok)
  end

  def init(:ok) do
    {:consumer, :ok}
  end

  def handle_events(events, _from, state) do
    for event <- events do
      IO.inspect(event, label: "Processed")
    end
    {:noreply, [], state}
  end
end
```

### Producer-Consumer Example

```elixir
defmodule EvenFilter do
  use GenStage

  def start_link() do
    GenStage.start_link(__MODULE__, :ok)
  end

  def init(:ok) do
    {:producer_consumer, :ok}
  end

  def handle_events(events, _from, state) do
    even_numbers = Enum.filter(events, &rem(&1, 2) == 0)
    {:noreply, even_numbers, state}
  end
end
```

## Distributed Queueing System Architecture

### Building a Kafka-like System

For building distributed queueing systems, consider this architecture:

```
[Data Source] -> [Producer Stage] -> [Partition Router] -> [Queue Managers] -> [Consumer Groups]
```

#### Key Components:

1. **Partition Router**: Routes messages to specific partitions based on keys
2. **Queue Managers**: Manage individual partition queues with persistence
3. **Consumer Groups**: Load balance processing across multiple consumers
4. **Coordination Layer**: Manages partition assignments and rebalancing

### Example Distributed Queue Architecture

```elixir
defmodule DistributedQueue.PartitionRouter do
  use GenStage

  def init({partitions, routing_strategy}) do
    {:producer_consumer, %{partitions: partitions, strategy: routing_strategy}}
  end

  def handle_events(events, _from, state) do
    partitioned_events = 
      events
      |> Enum.group_by(&route_to_partition(&1, state.strategy))
      |> Enum.map(fn {partition, partition_events} ->
        {partition, partition_events}
      end)
    
    {:noreply, partitioned_events, state}
  end
end
```

## Flow: Apache Beam's PCollection Equivalent

### Overview
Flow provides computational flows with parallel processing capabilities, similar to Apache Beam's PCollection transformations.

### Key Features

- **Parallel Transformations**: Map, filter, reduce operations across multiple cores
- **Windowing**: Time-based event processing for streaming data
- **Partitioning**: Automatic data distribution across stages
- **Bounded/Unbounded Data**: Supports both finite and infinite data streams

### Flow vs Apache Beam Comparison

| Feature | Apache Beam PCollection | Elixir Flow |
|---------|------------------------|-------------|
| Parallel Processing | ✓ | ✓ |
| Windowing | ✓ | ✓ |
| State Management | ✓ | ✓ |
| Fault Tolerance | Runner-dependent | BEAM VM built-in |
| Language Support | Multi-language | Elixir only |
| Distributed Execution | Multi-cluster | Single BEAM cluster |

### Flow Example: Word Count (Apache Beam equivalent)

```elixir
# Apache Beam style word count in Elixir Flow
File.stream!("large_text_file.txt")
|> Flow.from_enumerable(stages: 4)
|> Flow.flat_map(&String.split(&1, ~r/\W+/))
|> Flow.filter(&(&1 != ""))
|> Flow.partition(key: &String.downcase/1)
|> Flow.reduce(fn -> %{} end, fn word, acc ->
  Map.update(acc, String.downcase(word), 1, &(&1 + 1))
end)
|> Flow.map(fn {word, count} -> "#{word}: #{count}" end)
|> Enum.to_list()
```

### Custom Lambda Transformations in Flow

```elixir
defmodule CustomTransforms do
  # Define custom transformation functions
  def normalize_data(data) do
    data
    |> String.trim()
    |> String.downcase()
  end

  def enrich_data(item, enrichment_table) do
    Map.merge(item, Map.get(enrichment_table, item.id, %{}))
  end
end

# Using custom transformations in Flow pipeline
data_stream
|> Flow.from_enumerable()
|> Flow.map(&CustomTransforms.normalize_data/1)
|> Flow.map(&CustomTransforms.enrich_data(&1, enrichment_data))
|> Flow.partition()
|> Flow.reduce(fn -> [] end, fn item, acc -> [item | acc] end)
|> Enum.to_list()
```

## Broadway: Production-Ready Data Ingestion

### Overview
Broadway builds on GenStage to provide production-ready data ingestion pipelines with built-in features for real-world applications.

### Key Features

- **Multiple Producers**: Kafka, SQS, RabbitMQ, Google PubSub support
- **Automatic Acknowledgments**: Built-in message acknowledgment handling
- **Batching**: Configurable batching by size and time
- **Rate Limiting**: Built-in back-pressure and rate limiting
- **Telemetry**: Comprehensive metrics and monitoring
- **Fault Tolerance**: Graceful error handling and recovery

### Broadway Pipeline Example

```elixir
defmodule DataProcessor do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {BroadwayKafka.Producer, [
          hosts: ["localhost:9092"],
          group_id: "data_processor_group",
          topics: ["user_events"]
        ]},
        concurrency: 2
      ],
      processors: [
        default: [concurrency: 4]
      ],
      batchers: [
        default: [
          batch_size: 100,
          batch_timeout: 2000,
          concurrency: 2
        ]
      ]
    )
  end

  def handle_message(processor, message, _context) do
    # Custom lambda processing logic
    processed_data = 
      message.data
      |> Jason.decode!()
      |> transform_user_event()
      |> enrich_with_metadata()

    message
    |> Message.update_data(processed_data)
    |> Message.put_batch_key(processed_data.user_id)
  end

  def handle_batch(:default, messages, _batch_info, _context) do
    # Batch processing logic
    messages
    |> Enum.map(&process_batch_item/1)
    |> store_to_database()
    
    messages
  end

  defp transform_user_event(event_data) do
    # Custom transformation logic
    %{
      user_id: event_data["user_id"],
      event_type: event_data["type"],
      timestamp: DateTime.utc_now(),
      processed: true
    }
  end

  defp enrich_with_metadata(event) do
    # Custom enrichment logic
    Map.put(event, :metadata, get_user_metadata(event.user_id))
  end
end
```

## Performance Characteristics

### Scalability Patterns

1. **Horizontal Scaling**: Add more consumer processes
2. **Partitioning**: Distribute work across multiple partitions
3. **Batching**: Process events in batches for efficiency
4. **Load Balancing**: Use ConsumerSupervisor for dynamic scaling

### Memory Management

- **Bounded Queues**: Control memory usage with max_demand
- **Back-pressure**: Automatic flow control prevents memory exhaustion
- **Selective Receive**: Efficient message processing in BEAM VM

### Fault Tolerance

- **Supervision Trees**: Automatic process restart on failures
- **Isolation**: Failures in one stage don't affect others
- **Let It Crash**: BEAM VM philosophy for robust systems

## Best Practices for Distributed Queueing Systems

### 1. Design Principles

- **Single Responsibility**: Each stage should have one clear purpose
- **Loose Coupling**: Stages should be independent and composable
- **Idempotent Operations**: Handle duplicate messages gracefully
- **Graceful Degradation**: System should continue operating during partial failures

### 2. Monitoring and Observability

```elixir
# Add telemetry for monitoring
defmodule QueueTelemetry do
  def attach_handlers do
    :telemetry.attach_many(
      "queue-metrics",
      [
        [:broadway, :processor, :start],
        [:broadway, :processor, :stop],
        [:broadway, :batcher, :start],
        [:broadway, :batcher, :stop]
      ],
      &handle_event/4,
      nil
    )
  end

  def handle_event([:broadway, :processor, :stop], measurements, metadata, _config) do
    duration = measurements.duration
    # Send metrics to monitoring system
    :telemetry_metrics.emit_metric("queue.processing.duration", duration)
  end
end
```

### 3. Testing Strategies

```elixir
defmodule QueueTest do
  use ExUnit.Case
  
  test "processes messages correctly" do
    {:ok, producer} = NumberProducer.start_link(0)
    {:ok, consumer} = PrinterConsumer.start_link()
    
    GenStage.sync_subscribe(consumer, to: producer, max_demand: 5)
    
    # Test message flow
    assert_receive {:processed, events} when length(events) == 5
  end
end
```

### 4. Configuration Management

```elixir
# config/config.exs
config :my_queue_system,
  producer_concurrency: 2,
  consumer_concurrency: 4,
  batch_size: 100,
  max_demand: 1000,
  partitions: 8
```

## Advanced Patterns

### 1. Dynamic Scaling

```elixir
defmodule DynamicConsumerSupervisor do
  use ConsumerSupervisor

  def start_link(producer) do
    ConsumerSupervisor.start_link(__MODULE__, producer)
  end

  def init(producer) do
    children = [%{
      id: Worker,
      start: {Worker, :start_link, []},
      restart: :temporary
    }]

    opts = [
      strategy: :one_for_one,
      subscribe_to: [{producer, max_demand: 10, min_demand: 5}]
    ]

    ConsumerSupervisor.init(children, opts)
  end
end
```

### 2. Persistent Queues with RocksDB

```elixir
defmodule PersistentQueue do
  use GenStage
  
  def init(db_path) do
    {:ok, db} = :rocksdb.open(db_path, [create_if_missing: true])
    {:producer, %{db: db, counter: 0}}
  end

  def handle_demand(demand, %{db: db, counter: counter} = state) do
    events = 
      counter..(counter + demand - 1)
      |> Enum.map(fn i ->
        case :rocksdb.get(db, "#{i}", []) do
          {:ok, data} -> Jason.decode!(data)
          :not_found -> generate_new_event(i)
        end
      end)
    
    {:noreply, events, %{state | counter: counter + demand}}
  end
end
```

### 3. Message Routing and Filtering

```elixir
defmodule MessageRouter do
  use GenStage

  def handle_events(messages, _from, routes) do
    routed_messages = 
      Enum.flat_map(messages, fn message ->
        routes
        |> Enum.filter(fn {condition, _target} -> condition.(message) end)
        |> Enum.map(fn {_condition, target} -> {target, message} end)
      end)

    {:noreply, routed_messages, routes}
  end
end
```

## Conclusion

GenStage, Flow, and Broadway provide a comprehensive ecosystem for building distributed queueing systems in Elixir. While they may not match Apache Beam's scale for massive multi-cluster deployments, they offer:

- **Native BEAM VM integration**: Built-in fault tolerance and concurrency
- **Excellent developer experience**: Simple, composable APIs
- **Production-ready features**: Monitoring, batching, acknowledgments
- **Flexible architecture**: Support for various messaging patterns

For building Kafka-like distributed queueing systems, this ecosystem provides robust primitives with the added benefits of Elixir's actor model and "let it crash" philosophy.

The combination of GenStage's back-pressure mechanisms, Flow's parallel processing capabilities, and Broadway's production features makes Elixir an excellent choice for distributed data processing applications that need to handle high-throughput, real-time data streams with strong consistency and fault tolerance guarantees.