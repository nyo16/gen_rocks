defmodule GenRocks.Examples do
  @moduledoc """
  Example usage patterns for the GenRocks distributed queue system.
  Demonstrates Apache Beam-like PCollection operations and distributed processing.
  
  For more comprehensive examples, see:
  - `GenRocks.Examples.StorageAdapterExamples` - Storage adapter usage and comparisons
  - `GenRocks.Examples.DataProcessingExamples` - Flow-based data processing pipelines
  - `GenRocks.Examples.DiskLogExamples` - DiskLog adapter specific examples
  """

  require Logger

  alias GenRocks.Queue.{Supervisor, TopicProducer, ConsumerGroup, Message, FlowProcessor}

  @doc """
  Basic producer-consumer example.
  Creates a topic, produces messages, and consumes them.
  """
  def basic_example do
    topic = "example_topic"
    partition_count = 4
    
    Logger.info("Starting basic example...")

    # Start topic with 4 partitions
    {:ok, _} = Supervisor.start_topic(topic, partition_count)

    # Produce some test messages
    produce_test_messages(topic, 100)

    # Start a consumer group
    consumer_fn = fn message, _context ->
      Logger.info("Consumed: #{inspect(message.value)}")
      :ok
    end

    {:ok, _} = Supervisor.start_consumer_group("test_group", topic, consumer_fn, 
      partitions: [0, 1, 2, 3]
    )

    # Let it run for a few seconds
    Process.sleep(5000)
    
    Logger.info("Basic example completed")
  end

  @doc """
  Flow processing example - word count (Apache Beam style).
  Demonstrates PCollection-like transformations.
  """
  def flow_word_count_example do
    topic = "text_topic"
    
    Logger.info("Starting Flow word count example...")

    # Start topic
    {:ok, _} = Supervisor.start_topic(topic, 2)

    # Produce text messages
    texts = [
      "hello world",
      "hello elixir",
      "distributed systems",
      "apache beam style",
      "flow processing",
      "hello world again"
    ]

    Enum.each(texts, fn text ->
      TopicProducer.publish(topic, text, key: "text_#{:rand.uniform(1000)}")
    end)

    # Process with Flow (Apache Beam PCollection style)
    result = 
      FlowProcessor.from_topic(topic, partition_count: 2)
      |> FlowProcessor.word_count()
      |> FlowProcessor.collect()

    Logger.info("Word count result: #{inspect(result)}")
    result
  end

  @doc """
  Custom transformation pipeline example.
  Shows lambda-style custom transformations.
  """
  def custom_transformation_example do
    topic = "events_topic"
    
    Logger.info("Starting custom transformation example...")

    # Start topic
    {:ok, _} = Supervisor.start_topic(topic, 2)

    # Produce event messages
    events = [
      %{type: "user_signup", user_id: 1, timestamp: DateTime.utc_now()},
      %{type: "user_login", user_id: 1, timestamp: DateTime.utc_now()},
      %{type: "purchase", user_id: 2, amount: 99.99, timestamp: DateTime.utc_now()},
      %{type: "user_signup", user_id: 3, timestamp: DateTime.utc_now()},
      %{type: "purchase", user_id: 1, amount: 49.99, timestamp: DateTime.utc_now()},
    ]

    Enum.each(events, fn event ->
      TopicProducer.publish(topic, event, key: "user_#{event.user_id}")
    end)

    # Custom transformation pipeline
    result = 
      FlowProcessor.from_topic(topic, partition_count: 2)
      |> FlowProcessor.filter(fn msg -> msg.value.type == "purchase" end)
      |> FlowProcessor.transform(fn msg -> 
        %{msg | value: %{msg.value | amount_cents: trunc(msg.value.amount * 100)}}
      end)
      |> FlowProcessor.group_by_key(fn msg -> msg.value.user_id end)
      |> FlowProcessor.reduce(
        fn -> %{total_spent: 0, purchase_count: 0} end,
        fn {_user_id, purchases}, acc ->
          total = Enum.reduce(purchases, 0, fn msg, sum -> 
            sum + msg.value.amount_cents 
          end)
          %{total_spent: total, purchase_count: length(purchases)}
        end
      )
      |> FlowProcessor.collect()

    Logger.info("Purchase analysis result: #{inspect(result)}")
    result
  end

  @doc """
  Real-time stream processing example with windowing.
  """
  def windowed_processing_example do
    topic = "metrics_topic"
    
    Logger.info("Starting windowed processing example...")

    # Start topic
    {:ok, _} = Supervisor.start_topic(topic, 1)

    # Simulate real-time metrics
    spawn(fn -> produce_metrics_stream(topic, 50) end)

    # Process with time windows
    result =
      FlowProcessor.from_topic(topic, partition_count: 1)
      |> FlowProcessor.window({:fixed, 2000})  # 2-second windows
      |> FlowProcessor.aggregate(:average, value_fn: fn msg -> msg.value.cpu_usage end)
      |> FlowProcessor.side_effect(fn avg ->
        Logger.info("2-second window average CPU: #{Float.round(avg, 2)}%")
      end)
      |> FlowProcessor.collect()

    Logger.info("Windowed processing completed")
    result
  end

  @doc """
  Multi-stage processing pipeline with branching.
  """
  def branched_pipeline_example do
    topic = "logs_topic"
    
    Logger.info("Starting branched pipeline example...")

    # Start topic
    {:ok, _} = Supervisor.start_topic(topic, 2)

    # Produce log messages
    log_levels = ["DEBUG", "INFO", "WARN", "ERROR"]
    
    1..50
    |> Enum.each(fn i ->
      level = Enum.random(log_levels)
      TopicProducer.publish(topic, %{
        level: level,
        message: "Log message #{i}",
        timestamp: DateTime.utc_now()
      }, key: "log_#{i}")
    end)

    # Branch processing based on log level
    base_flow = FlowProcessor.from_topic(topic, partition_count: 2)
    
    branches = FlowProcessor.branch(base_flow, [
      {:errors, fn msg -> 
        if msg.value.level == "ERROR" do
          # Simulate alert processing
          Logger.error("ALERT: #{msg.value.message}")
          %{alert: true, original: msg}
        else
          nil
        end
      end},
      {:metrics, fn msg ->
        %{level: msg.value.level, count: 1}
      end}
    ])

    results = Enum.map(branches, fn {name, flow} ->
      {name, FlowProcessor.collect(flow)}
    end)

    Logger.info("Branched pipeline results: #{inspect(results)}")
    results
  end

  @doc """
  Performance test with high message throughput.
  """
  def performance_test(message_count \\ 10_000) do
    topic = "perf_topic"
    partition_count = 8
    
    Logger.info("Starting performance test with #{message_count} messages...")

    # Start topic with more partitions for better parallelism
    {:ok, _} = Supervisor.start_topic(topic, partition_count)

    # Start timer
    start_time = System.monotonic_time(:millisecond)

    # Produce messages in parallel
    tasks = 
      0..(partition_count - 1)
      |> Enum.map(fn partition ->
        Task.async(fn ->
          messages_per_partition = div(message_count, partition_count)
          produce_performance_messages(topic, partition, messages_per_partition)
        end)
      end)

    # Wait for all producers to finish
    Task.await_many(tasks, 30_000)
    
    produce_time = System.monotonic_time(:millisecond) - start_time
    produce_rate = message_count / (produce_time / 1000)
    
    Logger.info("Produced #{message_count} messages in #{produce_time}ms (#{Float.round(produce_rate, 2)} msg/sec)")

    # Start consumer and measure consumption
    consume_start = System.monotonic_time(:millisecond)
    consumed_count = :atomics.new(1, [])
    
    consumer_fn = fn _message, _context ->
      :atomics.add(consumed_count, 1, 1)
      :ok
    end

    {:ok, _} = Supervisor.start_consumer_group("perf_group", topic, consumer_fn,
      partitions: 0..(partition_count - 1) |> Enum.to_list(),
      max_demand: 100
    )

    # Wait for consumption to complete
    wait_for_consumption(consumed_count, message_count, consume_start)
    
    total_time = System.monotonic_time(:millisecond) - start_time
    Logger.info("Total test completed in #{total_time}ms")
    
    %{
      messages: message_count,
      partitions: partition_count,
      produce_time_ms: produce_time,
      total_time_ms: total_time,
      produce_rate: produce_rate
    }
  end

  # Helper functions

  defp produce_test_messages(topic, count) do
    1..count
    |> Enum.each(fn i ->
      TopicProducer.publish(topic, %{id: i, data: "test_message_#{i}"}, 
        key: "msg_#{i}"
      )
    end)
  end

  defp produce_metrics_stream(topic, count) do
    1..count
    |> Enum.each(fn i ->
      metric = %{
        cpu_usage: :rand.uniform() * 100,
        memory_usage: :rand.uniform() * 100,
        timestamp: DateTime.utc_now()
      }
      
      TopicProducer.publish(topic, metric, key: "metric_#{i}")
      Process.sleep(100)  # Simulate real-time streaming
    end)
  end

  defp produce_performance_messages(topic, partition_hint, count) do
    1..count
    |> Enum.each(fn i ->
      TopicProducer.publish(topic, %{
        partition: partition_hint,
        id: i,
        data: "perf_message_#{partition_hint}_#{i}",
        timestamp: DateTime.utc_now()
      }, key: "perf_#{partition_hint}_#{i}")
    end)
  end

  defp wait_for_consumption(counter, expected, start_time, timeout \\ 30_000) do
    consumed = :atomics.get(counter, 1)
    elapsed = System.monotonic_time(:millisecond) - start_time
    
    cond do
      consumed >= expected ->
        consume_time = elapsed
        consume_rate = consumed / (consume_time / 1000)
        Logger.info("Consumed #{consumed} messages in #{consume_time}ms (#{Float.round(consume_rate, 2)} msg/sec)")
        
      elapsed > timeout ->
        Logger.warning("Consumption timeout after #{timeout}ms, consumed #{consumed}/#{expected}")
        
      true ->
        Process.sleep(100)
        wait_for_consumption(counter, expected, start_time, timeout)
    end
  end
end