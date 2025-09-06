defmodule GenRocks.Examples.DiskLogExamples do
  @moduledoc """
  Examples demonstrating the usage of the disk_log storage adapter.
  
  The disk_log adapter provides durable storage with Write-Ahead Log (WAL) semantics,
  automatic file rotation, and crash recovery capabilities.
  """

  require Logger

  @doc """
  Basic example of using disk_log storage for a topic.
  
  This creates a topic with disk_log storage, publishes some messages,
  and demonstrates that they persist across restarts.
  """
  def basic_disk_log_example do
    Logger.info("=== Basic Disk Log Example ===")
    
    # Start a topic with disk_log storage
    {:ok, _} = GenRocks.start_topic_with_disk_log("orders", 2, 
      log_dir: "./example_logs",
      max_no_files: 5,
      max_no_bytes: 1024 * 1024  # 1MB per file
    )
    
    # Publish some messages
    orders = [
      %{order_id: "ORDER-001", customer: "Alice", amount: 99.99, product: "Laptop"},
      %{order_id: "ORDER-002", customer: "Bob", amount: 29.99, product: "Book"},
      %{order_id: "ORDER-003", customer: "Carol", amount: 199.99, product: "Phone"}
    ]
    
    Enum.each(orders, fn order ->
      GenRocks.publish("orders", order)
      Logger.info("Published order: #{order.order_id}")
    end)
    
    # Start a consumer to process messages
    consumer_fn = fn message, _context ->
      Logger.info("Processing order: #{inspect(message.value)}")
      :ok
    end
    
    {:ok, _consumer} = GenRocks.start_consumer_group(
      "orders", "order_processor", consumer_fn
    )
    
    # Let it process for a bit
    Process.sleep(2000)
    
    Logger.info("Orders persisted to disk in ./example_logs/orders/")
    
    # Stop the topic
    GenRocks.stop_topic("orders")
    
    {:ok, :completed}
  end
  
  @doc """
  Example showing disk_log with high throughput and automatic file rotation.
  """
  def high_throughput_example do
    Logger.info("=== High Throughput Disk Log Example ===")
    
    # Start topic with smaller file sizes to demonstrate rotation
    {:ok, _} = GenRocks.start_topic_with_disk_log("events", 4,
      log_dir: "./high_throughput_logs",
      max_no_files: 10,
      max_no_bytes: 64 * 1024  # 64KB per file (small for demo)
    )
    
    # Generate high volume of events
    event_count = 1000
    
    Logger.info("Generating #{event_count} events...")
    
    for i <- 1..event_count do
      event = %{
        event_id: "EVENT-#{String.pad_leading(to_string(i), 6, "0")}",
        timestamp: System.system_time(:millisecond),
        user_id: rem(i, 100),  # 100 different users
        action: Enum.random(["login", "logout", "purchase", "view", "click"]),
        metadata: %{
          session_id: "session-#{rem(i, 50)}",
          ip_address: "192.168.1.#{rem(i, 255)}",
          user_agent: "Browser/1.0"
        }
      }
      
      GenRocks.publish("events", event)
      
      # Log progress every 100 events
      if rem(i, 100) == 0 do
        Logger.info("Published #{i}/#{event_count} events")
      end
    end
    
    Logger.info("All events published. Files will be in ./high_throughput_logs/events/")
    
    # Show directory structure
    Logger.info("Directory structure:")
    
    case File.ls("./high_throughput_logs/events") do
      {:ok, partitions} ->
        Enum.each(partitions, fn partition_dir ->
          partition_path = Path.join(["./high_throughput_logs/events", partition_dir])
          case File.ls(partition_path) do
            {:ok, files} ->
              Logger.info("  #{partition_dir}/: #{inspect(files)}")
            _ ->
              :ok
          end
        end)
      _ ->
        Logger.info("  Directory not yet created or accessible")
    end
    
    GenRocks.stop_topic("events")
    
    {:ok, :completed}
  end
  
  @doc """
  Example demonstrating crash recovery with disk_log storage.
  
  This simulates a crash and restart scenario to show how data persists.
  """
  def crash_recovery_example do
    Logger.info("=== Crash Recovery Example ===")
    
    # Phase 1: Write some critical data
    Logger.info("Phase 1: Writing critical transaction data...")
    
    {:ok, _} = GenRocks.start_topic_with_disk_log("transactions", 1,
      log_dir: "./recovery_logs"
    )
    
    # Write critical transactions
    transactions = [
      %{tx_id: "TX-001", amount: 1000.00, from: "account-1", to: "account-2"},
      %{tx_id: "TX-002", amount: 250.00, from: "account-3", to: "account-1"},
      %{tx_id: "TX-003", amount: 750.00, from: "account-2", to: "account-4"}
    ]
    
    Enum.each(transactions, fn tx ->
      GenRocks.publish("transactions", tx)
      Logger.info("Recorded transaction: #{tx.tx_id}")
    end)
    
    # Simulate normal processing time
    Process.sleep(1000)
    
    # Phase 2: Simulate crash by stopping the topic abruptly
    Logger.info("Phase 2: Simulating system crash...")
    GenRocks.stop_topic("transactions")
    
    # Wait a bit to simulate downtime
    Process.sleep(1000)
    
    # Phase 3: Restart and verify data recovery
    Logger.info("Phase 3: Restarting system and recovering data...")
    
    {:ok, _} = GenRocks.start_topic_with_disk_log("transactions", 1,
      log_dir: "./recovery_logs"  # Same log directory
    )
    
    # Start consumer to verify data recovery
    recovered_transactions = []
    
    consumer_fn = fn message, context ->
      Logger.info("Recovered transaction: #{inspect(message.value)}")
      # In a real scenario, you'd rebuild your application state here
      :ok
    end
    
    {:ok, _consumer} = GenRocks.start_consumer_group(
      "transactions", "recovery_processor", consumer_fn
    )
    
    # Wait for recovery processing
    Process.sleep(2000)
    
    Logger.info("Crash recovery completed. All transactions recovered from WAL.")
    
    GenRocks.stop_topic("transactions")
    
    {:ok, :completed}
  end
  
  @doc """
  Example showing different log configurations and their trade-offs.
  """
  def configuration_comparison_example do
    Logger.info("=== Configuration Comparison Example ===")
    
    # Configuration 1: High durability (many small files)
    Logger.info("Starting topic with high durability configuration...")
    {:ok, _} = GenRocks.start_topic_with_disk_log("durable_events", 1,
      log_dir: "./config_comparison/durable",
      max_no_files: 20,
      max_no_bytes: 32 * 1024  # 32KB files
    )
    
    # Configuration 2: High performance (fewer large files)
    Logger.info("Starting topic with high performance configuration...")
    {:ok, _} = GenRocks.start_topic_with_disk_log("fast_events", 1,
      log_dir: "./config_comparison/fast",
      max_no_files: 3,
      max_no_bytes: 10 * 1024 * 1024  # 10MB files
    )
    
    # Publish the same data to both topics
    test_data = for i <- 1..200 do
      %{
        id: i,
        data: String.duplicate("x", 100),  # 100 bytes per message
        timestamp: System.system_time(:millisecond)
      }
    end
    
    Logger.info("Publishing #{length(test_data)} messages to both configurations...")
    
    # Measure performance for durable configuration
    {durable_time, :ok} = :timer.tc(fn ->
      Enum.each(test_data, fn data ->
        GenRocks.publish("durable_events", data)
      end)
    end)
    
    # Measure performance for fast configuration
    {fast_time, :ok} = :timer.tc(fn ->
      Enum.each(test_data, fn data ->
        GenRocks.publish("fast_events", data)
      end)
    end)
    
    Logger.info("Performance comparison:")
    Logger.info("  Durable config: #{durable_time / 1000} ms")
    Logger.info("  Fast config: #{fast_time / 1000} ms")
    
    # Show file structures
    Logger.info("File structure comparison:")
    
    ["./config_comparison/durable/durable_events", "./config_comparison/fast/fast_events"]
    |> Enum.each(fn dir ->
      case File.ls(dir) do
        {:ok, partitions} ->
          Enum.each(partitions, fn partition ->
            partition_path = Path.join([dir, partition])
            case File.ls(partition_path) do
              {:ok, files} ->
                Logger.info("  #{dir}/#{partition}: #{length(files)} files")
              _ -> :ok
            end
          end)
        _ ->
          Logger.info("  #{dir}: not accessible")
      end
    end)
    
    GenRocks.stop_topic("durable_events")
    GenRocks.stop_topic("fast_events")
    
    {:ok, :completed}
  end
  
  @doc """
  Runs all disk_log examples in sequence.
  """
  def run_all_examples do
    Logger.info("Running all disk_log examples...")
    
    examples = [
      {:basic_disk_log_example, "Basic Usage"},
      {:high_throughput_example, "High Throughput"},
      {:crash_recovery_example, "Crash Recovery"}, 
      {:configuration_comparison_example, "Configuration Comparison"}
    ]
    
    results = Enum.map(examples, fn {example_fn, name} ->
      Logger.info("\\n" <> String.duplicate("=", 50))
      Logger.info("Running: #{name}")
      Logger.info(String.duplicate("=", 50))
      
      result = apply(__MODULE__, example_fn, [])
      
      Logger.info("#{name}: #{inspect(result)}")
      {example_fn, result}
    end)
    
    Logger.info("\\n" <> String.duplicate("=", 50))
    Logger.info("All examples completed!")
    Logger.info("Check the following directories for generated log files:")
    Logger.info("  - ./example_logs/")
    Logger.info("  - ./high_throughput_logs/")
    Logger.info("  - ./recovery_logs/")
    Logger.info("  - ./config_comparison/")
    Logger.info(String.duplicate("=", 50))
    
    {:ok, results}
  end
  
  @doc """
  Cleans up all example log directories.
  """
  def cleanup_examples do
    dirs_to_clean = [
      "./example_logs",
      "./high_throughput_logs", 
      "./recovery_logs",
      "./config_comparison"
    ]
    
    Enum.each(dirs_to_clean, fn dir ->
      if File.exists?(dir) do
        File.rm_rf!(dir)
        Logger.info("Cleaned up #{dir}")
      end
    end)
    
    :ok
  end
end