defmodule GenRocks.Examples.StorageAdapterExamples do
  @moduledoc """
  Comprehensive examples showing how to use different storage adapters in GenRocks.
  
  This module demonstrates:
  - ETS Adapter: High-performance in-memory storage for development/testing
  - DiskLog Adapter: Durable Write-Ahead Log storage for production
  - Adapter comparison and selection guidance
  - Migration between adapters
  - Performance testing and benchmarking
  """
  
  require Logger
  
  @doc """
  Demonstrates basic usage of the ETS adapter.
  
  ETS is best for:
  - Development and testing
  - High-performance scenarios where data loss is acceptable
  - Temporary processing pipelines
  - Cache-like workloads
  """
  def ets_adapter_example do
    Logger.info("=== ETS Adapter Example ===")
    
    # Start topic with ETS adapter (default)
    {:ok, _} = GenRocks.start_topic("fast_processing", 2)
    
    # Publish high-volume messages
    messages = for i <- 1..1000 do
      %{
        id: i,
        timestamp: System.system_time(:millisecond),
        data: "Fast message #{i}",
        priority: rem(i, 3)
      }
    end
    
    Logger.info("Publishing #{length(messages)} messages to ETS adapter...")
    
    {time_ms, :ok} = :timer.tc(fn ->
      Enum.each(messages, fn msg ->
        GenRocks.publish("fast_processing", msg)
      end)
    end)
    
    Logger.info("ETS publish time: #{time_ms / 1000} ms (#{length(messages) / (time_ms / 1_000_000)} msg/sec)")
    
    # Start consumer
    consumer_fn = fn message, _context ->
      if rem(message.value.id, 100) == 0 do
        Logger.info("Processed message: #{message.value.id}")
      end
      :ok
    end
    
    {:ok, _consumer} = GenRocks.start_consumer_group(
      "fast_processing", "fast_consumer", consumer_fn
    )
    
    Process.sleep(2000)
    
    Logger.info("ETS adapter demo completed - data exists only in memory")
    GenRocks.stop_topic("fast_processing")
    
    {:ok, :completed}
  end
  
  @doc """
  Demonstrates the DiskLog adapter with different configurations.
  
  DiskLog is best for:
  - Production environments requiring durability
  - Event sourcing and audit trails
  - Systems that must survive crashes
  - Long-term data retention
  """
  def disk_log_adapter_example do
    Logger.info("=== DiskLog Adapter Example ===")
    
    # Configuration 1: Development setup (small files for quick testing)
    dev_config = [
      log_dir: "./adapter_examples/dev_logs",
      max_no_files: 3,
      max_no_bytes: 1024 * 1024  # 1MB files
    ]
    
    Logger.info("Starting topic with development DiskLog configuration...")
    {:ok, _} = GenRocks.start_topic_with_disk_log("dev_orders", 2, dev_config)
    
    # Configuration 2: Production setup (large files, high durability)  
    prod_config = [
      log_dir: "./adapter_examples/prod_logs",
      max_no_files: 20,
      max_no_bytes: 100 * 1024 * 1024  # 100MB files
    ]
    
    Logger.info("Starting topic with production DiskLog configuration...")
    {:ok, _} = GenRocks.start_topic_with_disk_log("prod_orders", 4, prod_config)
    
    # Publish critical business events
    critical_events = [
      %{event_type: "order_created", order_id: "ORD-001", amount: 1299.99, customer: "alice@example.com"},
      %{event_type: "payment_processed", order_id: "ORD-001", payment_method: "credit_card", amount: 1299.99},
      %{event_type: "inventory_updated", product_id: "PROD-123", old_quantity: 100, new_quantity: 99},
      %{event_type: "order_shipped", order_id: "ORD-001", tracking_number: "TRK-456", carrier: "FastShip"}
    ]
    
    Logger.info("Publishing critical business events...")
    
    Enum.each(critical_events, fn event ->
      # Publish to both dev and prod topics to show configuration differences
      GenRocks.publish("dev_orders", event)
      GenRocks.publish("prod_orders", event)
      Logger.info("Published: #{event.event_type}")
    end)
    
    # Show that data persists across restarts
    Logger.info("Stopping topics to simulate restart...")
    GenRocks.stop_topic("dev_orders")
    GenRocks.stop_topic("prod_orders")
    
    Process.sleep(1000)
    
    Logger.info("Restarting topics - data should be recovered from disk logs...")
    {:ok, _} = GenRocks.start_topic_with_disk_log("dev_orders", 2, dev_config)
    {:ok, _} = GenRocks.start_topic_with_disk_log("prod_orders", 4, prod_config)
    
    # Verify data recovery with consumers
    recovery_consumer_fn = fn message, _context ->
      Logger.info("Recovered event: #{message.value.event_type} - #{inspect(message.value)}")
      :ok
    end
    
    {:ok, _consumer1} = GenRocks.start_consumer_group("dev_orders", "recovery_test", recovery_consumer_fn)
    {:ok, _consumer2} = GenRocks.start_consumer_group("prod_orders", "recovery_test", recovery_consumer_fn)
    
    Process.sleep(3000)
    
    GenRocks.stop_topic("dev_orders")
    GenRocks.stop_topic("prod_orders")
    
    Logger.info("DiskLog adapter demo completed - data persisted to disk")
    
    {:ok, :completed}
  end
  
  @doc """
  Demonstrates adapter performance comparison.
  Benchmarks different adapters under various workloads.
  """
  def adapter_performance_comparison do
    Logger.info("=== Adapter Performance Comparison ===")
    
    message_counts = [100, 1000, 5000]
    
    results = Enum.map(message_counts, fn count ->
      Logger.info("Testing with #{count} messages...")
      
      # Test ETS adapter
      {:ok, _} = GenRocks.start_topic("ets_perf_test", 1)
      
      messages = for i <- 1..count do
        %{id: i, data: String.duplicate("x", 100), timestamp: System.system_time(:millisecond)}
      end
      
      {ets_time, :ok} = :timer.tc(fn ->
        Enum.each(messages, fn msg ->
          GenRocks.publish("ets_perf_test", msg)
        end)
      end)
      
      GenRocks.stop_topic("ets_perf_test")
      
      # Test DiskLog adapter
      {:ok, _} = GenRocks.start_topic_with_disk_log("disk_perf_test", 1,
        log_dir: "./adapter_examples/perf_test",
        max_no_bytes: 50 * 1024 * 1024  # 50MB
      )
      
      {disk_time, :ok} = :timer.tc(fn ->
        Enum.each(messages, fn msg ->
          GenRocks.publish("disk_perf_test", msg)
        end)
      end)
      
      GenRocks.stop_topic("disk_perf_test")
      
      ets_throughput = count / (ets_time / 1_000_000)
      disk_throughput = count / (disk_time / 1_000_000)
      
      Logger.info("#{count} messages:")
      Logger.info("  ETS: #{Float.round(ets_time / 1000, 2)} ms (#{Float.round(ets_throughput, 0)} msg/sec)")
      Logger.info("  DiskLog: #{Float.round(disk_time / 1000, 2)} ms (#{Float.round(disk_throughput, 0)} msg/sec)")
      Logger.info("  Overhead: #{Float.round((disk_time / ets_time - 1) * 100, 1)}%")
      
      %{
        message_count: count,
        ets_time_ms: ets_time / 1000,
        disk_time_ms: disk_time / 1000,
        ets_throughput: ets_throughput,
        disk_throughput: disk_throughput,
        overhead_percent: (disk_time / ets_time - 1) * 100
      }
    end)
    
    Logger.info("Performance comparison completed:")
    Logger.info("Results: #{inspect(results)}")
    
    {:ok, results}
  end
  
  @doc """
  Demonstrates how to choose the right adapter for different use cases.
  """
  def adapter_selection_guide do
    Logger.info("=== Storage Adapter Selection Guide ===")
    
    use_cases = [
      %{
        name: "Development & Testing",
        description: "Fast iteration, temporary data",
        recommended: "ETS",
        reasoning: "No disk I/O overhead, easy cleanup, fast restarts"
      },
      %{
        name: "High-Frequency Trading",
        description: "Ultra-low latency, acceptable data loss",
        recommended: "ETS", 
        reasoning: "Sub-millisecond latency, highest throughput"
      },
      %{
        name: "Financial Transactions",
        description: "Must never lose data, audit requirements",
        recommended: "DiskLog",
        reasoning: "ACID properties, crash recovery, persistent audit trail"
      },
      %{
        name: "IoT Data Processing", 
        description: "High volume sensor data, some loss acceptable",
        recommended: "ETS for hot data, DiskLog for aggregates",
        reasoning: "Process recent data fast, persist summaries for analysis"
      },
      %{
        name: "Event Sourcing",
        description: "Immutable event log, system of record",
        recommended: "DiskLog",
        reasoning: "Write-ahead log semantics, perfect for event streams"
      },
      %{
        name: "Caching Layer",
        description: "Temporary acceleration, source of truth elsewhere",
        recommended: "ETS",
        reasoning: "Memory speed, automatic cleanup on restart"
      },
      %{
        name: "Message Queue", 
        description: "Reliable delivery between services",
        recommended: "DiskLog",
        reasoning: "Durability guarantees, handles service restarts"
      }
    ]
    
    Enum.each(use_cases, fn use_case ->
      Logger.info("Use Case: #{use_case.name}")
      Logger.info("  Description: #{use_case.description}")
      Logger.info("  Recommended: #{use_case.recommended}")
      Logger.info("  Reasoning: #{use_case.reasoning}")
      Logger.info("")
    end)
    
    {:ok, use_cases}
  end
  
  @doc """
  Demonstrates hybrid approaches using multiple adapters.
  """
  def hybrid_adapter_example do
    Logger.info("=== Hybrid Adapter Architecture Example ===")
    
    # Scenario: E-commerce system with different durability requirements
    
    # 1. Shopping cart - temporary data, high performance (ETS)
    {:ok, _} = GenRocks.start_topic("shopping_carts", 4)  # ETS default
    
    # 2. Orders - critical business data, must persist (DiskLog)
    {:ok, _} = GenRocks.start_topic_with_disk_log("orders", 4, 
      log_dir: "./adapter_examples/orders",
      max_no_bytes: 50 * 1024 * 1024
    )
    
    # 3. Analytics events - high volume, some loss acceptable but want to keep hot data (ETS)
    {:ok, _} = GenRocks.start_topic("analytics_events", 8)
    
    # 4. Audit log - compliance requirement, never lose (DiskLog)
    {:ok, _} = GenRocks.start_topic_with_disk_log("audit_log", 2,
      log_dir: "./adapter_examples/audit", 
      max_no_files: 50,  # Long retention
      max_no_bytes: 100 * 1024 * 1024  # Large files
    )
    
    Logger.info("Started hybrid architecture with 4 topics using different adapters")
    
    # Simulate user activity
    user_session = %{user_id: "user_123", session_id: "sess_456"}
    
    # Add items to cart (ETS - fast, temporary)
    GenRocks.publish("shopping_carts", %{
      action: "add_item",
      user_id: user_session.user_id,
      item_id: "PROD-789",
      quantity: 2,
      price: 29.99
    })
    
    # Track analytics (ETS - high volume, recent data most important)
    GenRocks.publish("analytics_events", %{
      event: "item_viewed", 
      user_id: user_session.user_id,
      item_id: "PROD-789",
      timestamp: System.system_time(:millisecond)
    })
    
    # Create order (DiskLog - critical business data)
    order_event = %{
      event_type: "order_created",
      order_id: "ORD-#{System.unique_integer([:positive])}",
      user_id: user_session.user_id,
      items: [%{item_id: "PROD-789", quantity: 2, price: 29.99}],
      total: 59.98,
      timestamp: System.system_time(:millisecond)
    }
    
    GenRocks.publish("orders", order_event)
    
    # Log for compliance (DiskLog - permanent audit trail)
    GenRocks.publish("audit_log", %{
      action: "order_created",
      user_id: user_session.user_id,
      order_id: order_event.order_id,
      amount: order_event.total,
      timestamp: order_event.timestamp,
      ip_address: "192.168.1.100"
    })
    
    Logger.info("Simulated user activity across all adapters")
    
    # Set up specialized consumers for each type
    
    # Fast cart processing
    GenRocks.start_consumer_group("shopping_carts", "cart_processor", fn msg, _ctx ->
      Logger.info("Cart update: #{msg.value.action} for user #{msg.value.user_id}")
      :ok
    end)
    
    # Reliable order processing  
    GenRocks.start_consumer_group("orders", "order_processor", fn msg, _ctx ->
      Logger.info("Processing order: #{msg.value.order_id} - $#{msg.value.total}")
      # In real system: payment processing, inventory updates, etc.
      :ok
    end)
    
    # Real-time analytics
    GenRocks.start_consumer_group("analytics_events", "analytics_processor", fn msg, _ctx ->
      # In real system: update dashboards, trigger recommendations
      :ok
    end)
    
    # Compliance processing
    GenRocks.start_consumer_group("audit_log", "compliance_processor", fn msg, _ctx ->
      Logger.info("Audit: #{msg.value.action} by user #{msg.value.user_id}")
      # In real system: regulatory reporting, fraud detection
      :ok
    end)
    
    Process.sleep(3000)
    
    Logger.info("Hybrid adapter architecture demo completed")
    Logger.info("Cart & Analytics: Fast ETS storage for temporary/high-volume data")  
    Logger.info("Orders & Audit: Durable DiskLog storage for critical data")
    
    # Cleanup
    GenRocks.stop_topic("shopping_carts")
    GenRocks.stop_topic("orders")  
    GenRocks.stop_topic("analytics_events")
    GenRocks.stop_topic("audit_log")
    
    {:ok, :completed}
  end
  
  @doc """
  Demonstrates adapter migration strategies.
  """
  def adapter_migration_example do
    Logger.info("=== Adapter Migration Example ===")
    
    # Scenario: Moving from development (ETS) to production (DiskLog)
    
    Logger.info("Phase 1: Development setup with ETS")
    {:ok, _} = GenRocks.start_topic("user_events", 2)  # ETS
    
    # Add some development data
    dev_events = [
      %{event: "user_signup", user_id: "dev_user_1", email: "test1@example.com"},
      %{event: "user_login", user_id: "dev_user_1", ip: "127.0.0.1"}, 
      %{event: "page_view", user_id: "dev_user_1", page: "/dashboard"},
      %{event: "user_signup", user_id: "dev_user_2", email: "test2@example.com"}
    ]
    
    Enum.each(dev_events, fn event ->
      GenRocks.publish("user_events", event)
    end)
    
    Logger.info("Published #{length(dev_events)} events to ETS adapter")
    
    # Consume and checkpoint current data
    processed_events = []
    
    consumer_fn = fn message, _context ->
      # In real migration: export to file, database, or other persistent store
      Logger.info("Checkpointing event: #{message.value.event}")
      :ok
    end
    
    {:ok, _consumer} = GenRocks.start_consumer_group("user_events", "migration_consumer", consumer_fn)
    Process.sleep(2000)
    
    GenRocks.stop_topic("user_events")
    
    Logger.info("Phase 2: Production migration to DiskLog")
    
    # Start with persistent storage
    {:ok, _} = GenRocks.start_topic_with_disk_log("user_events", 2,
      log_dir: "./adapter_examples/migration",
      max_no_bytes: 10 * 1024 * 1024
    )
    
    # In real scenario: reload checkpointed data
    Logger.info("Reloading data into production adapter...")
    
    Enum.each(dev_events, fn event ->
      # Add migration metadata
      production_event = Map.put(event, :migrated_at, System.system_time(:millisecond))
      GenRocks.publish("user_events", production_event)
    end)
    
    # Continue with new production events
    prod_events = [
      %{event: "user_purchase", user_id: "dev_user_1", amount: 99.99, product: "premium_plan"},
      %{event: "user_login", user_id: "dev_user_2", ip: "192.168.1.50"}
    ]
    
    Enum.each(prod_events, fn event ->
      GenRocks.publish("user_events", event) 
    end)
    
    Logger.info("Published #{length(prod_events)} new production events")
    
    # Verify migration with consumer
    GenRocks.start_consumer_group("user_events", "prod_consumer", fn msg, _ctx ->
      if Map.has_key?(msg.value, :migrated_at) do
        Logger.info("Migrated event: #{msg.value.event}")
      else
        Logger.info("New production event: #{msg.value.event}")
      end
      :ok
    end)
    
    Process.sleep(3000)
    
    GenRocks.stop_topic("user_events")
    
    Logger.info("Migration completed: ETS -> DiskLog")
    Logger.info("Benefits gained: Data persistence, crash recovery, audit trail")
    
    {:ok, :completed}
  end
  
  @doc """
  Demonstrates advanced DiskLog configurations for specific scenarios.
  """
  def advanced_disk_log_configurations do
    Logger.info("=== Advanced DiskLog Configurations ===")
    
    configurations = [
      {
        "High Throughput (Batch Processing)",
        [
          log_dir: "./adapter_examples/high_throughput",
          max_no_files: 5,
          max_no_bytes: 500 * 1024 * 1024,  # 500MB files
          log_type: :wrap
        ],
        "Large files reduce rotation overhead for high-volume scenarios"
      },
      {
        "High Durability (Financial)",
        [
          log_dir: "./adapter_examples/financial",
          max_no_files: 100,  # Long retention
          max_no_bytes: 10 * 1024 * 1024,  # 10MB files  
          log_type: :wrap
        ],
        "Many small files provide granular recovery and long retention"
      },
      {
        "Single File (Append Only)",
        [
          log_dir: "./adapter_examples/append_only",
          max_no_bytes: 1024 * 1024 * 1024,  # 1GB max
          log_type: :halt
        ],
        "Single growing file for simple append-only logs without rotation"
      }
    ]
    
    Enum.with_index(configurations, fn {name, config, description}, index ->
      topic_name = "config_test_#{index}"
      
      Logger.info("Configuration: #{name}")
      Logger.info("  Description: #{description}")
      Logger.info("  Config: #{inspect(config)}")
      
      {:ok, _} = GenRocks.start_topic_with_disk_log(topic_name, 1, config)
      
      # Test with appropriate workload
      test_messages = case name do
        "High Throughput" <> _ ->
          # Large batch of small messages
          for i <- 1..1000, do: %{batch_id: div(i, 100), seq: i, data: "msg_#{i}"}
          
        "High Durability" <> _ ->
          # Critical financial transactions
          for i <- 1..10 do
            %{
              transaction_id: "TXN-#{String.pad_leading(to_string(i), 6, "0")}",
              from_account: "ACC-#{rem(i, 3) + 1}",
              to_account: "ACC-#{rem(i + 1, 3) + 1}",
              amount: Float.round(:rand.uniform() * 1000, 2),
              timestamp: System.system_time(:millisecond)
            }
          end
          
        "Single File" <> _ ->
          # Audit events
          for i <- 1..50 do
            %{
              audit_id: "AUD-#{i}",
              user_id: "user_#{rem(i, 5)}",
              action: Enum.random(["login", "logout", "view", "edit", "delete"]),
              resource: "resource_#{rem(i, 10)}",
              timestamp: System.system_time(:millisecond)
            }
          end
      end
      
      Enum.each(test_messages, fn msg ->
        GenRocks.publish(topic_name, msg)
      end)
      
      Logger.info("  Published #{length(test_messages)} test messages")
      
      # Quick consumer to verify
      GenRocks.start_consumer_group(topic_name, "config_test_consumer", fn msg, _ctx ->
        if rem(msg.offset, 100) == 0 or msg.offset <= 5 do
          Logger.info("    Consumed: #{inspect(msg.value)}")
        end
        :ok
      end)
      
      Process.sleep(1500)
      GenRocks.stop_topic(topic_name)
      
      Logger.info("  Configuration test completed\\n")
    end)
    
    {:ok, :completed}
  end
  
  @doc """
  Runs all storage adapter examples in sequence.
  """
  def run_all_examples do
    Logger.info("Running all storage adapter examples...")
    
    examples = [
      {:ets_adapter_example, "ETS Adapter Basics"},
      {:disk_log_adapter_example, "DiskLog Adapter Basics"}, 
      {:adapter_performance_comparison, "Performance Comparison"},
      {:adapter_selection_guide, "Selection Guide"},
      {:hybrid_adapter_example, "Hybrid Architecture"},
      {:adapter_migration_example, "Adapter Migration"},
      {:advanced_disk_log_configurations, "Advanced DiskLog Configurations"}
    ]
    
    results = Enum.map(examples, fn {example_fn, name} ->
      Logger.info("\\n" <> String.duplicate("=", 60))
      Logger.info("Running: #{name}")
      Logger.info(String.duplicate("=", 60))
      
      result = apply(__MODULE__, example_fn, [])
      
      Logger.info("#{name}: #{inspect(result)}")
      {example_fn, result}
    end)
    
    Logger.info("\\n" <> String.duplicate("=", 60))
    Logger.info("All storage adapter examples completed!")
    Logger.info("Check ./adapter_examples/ for generated files and logs")
    Logger.info(String.duplicate("=", 60))
    
    {:ok, results}
  end
  
  @doc """
  Cleans up all example directories and test data.
  """
  def cleanup_examples do
    dirs_to_clean = [
      "./adapter_examples"
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