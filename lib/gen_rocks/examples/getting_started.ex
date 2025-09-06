defmodule GenRocks.Examples.GettingStarted do
  @moduledoc """
  Getting started guide for GenRocks storage adapters.
  
  This module provides step-by-step examples showing how to:
  1. Choose the right storage adapter for your use case
  2. Set up topics with different adapters
  3. Understand the trade-offs between adapters
  4. Migrate between adapters
  5. Best practices and common patterns
  
  Run `GenRocks.Examples.GettingStarted.interactive_guide()` for a guided tour.
  """
  
  require Logger
  
  @doc """
  Interactive guide that walks through adapter selection and usage.
  """
  def interactive_guide do
    Logger.info("""
    
    ╔══════════════════════════════════════════════════════════════╗
    ║                     GenRocks Storage Adapters                ║  
    ║                      Getting Started Guide                   ║
    ╚══════════════════════════════════════════════════════════════╝
    
    GenRocks supports multiple storage adapters for different use cases:
    
    📦 ETS Adapter - In-memory, high-performance
    💾 DiskLog Adapter - Persistent, crash-safe with WAL semantics
    
    Let's explore each one with practical examples...
    """)
    
    step_1_ets_basics()
    step_2_disk_log_basics()
    step_3_choosing_adapter()
    step_4_advanced_patterns()
    
    Logger.info("""
    
    🎉 Congratulations! You've completed the GenRocks storage adapter guide.
    
    Next steps:
    - Explore GenRocks.Examples.StorageAdapterExamples for detailed use cases
    - Try GenRocks.Examples.DataProcessingExamples for Flow-based processing
    - Check GenRocks.Examples.DiskLogExamples for advanced DiskLog features
    
    Happy queueing! 🚀
    """)
    
    {:ok, :completed}
  end
  
  defp step_1_ets_basics do
    Logger.info("""
    
    ┌─ Step 1: ETS Adapter Basics ─────────────────────────────────┐
    │                                                              │
    │ ETS (Erlang Term Storage) is perfect for:                   │
    │ • Development and testing                                    │  
    │ • High-performance scenarios                                 │
    │ • Temporary data processing                                  │
    │ • Cache-like workloads                                       │
    │                                                              │
    └──────────────────────────────────────────────────────────────┘
    """)
    
    Logger.info("Creating topic with ETS adapter (default)...")
    
    # ETS is the default adapter
    {:ok, _} = GenRocks.start_topic("demo_ets", 2)
    
    # Publish some messages
    messages = [
      %{id: 1, message: "Hello from ETS!", timestamp: System.system_time(:millisecond)},
      %{id: 2, message: "Fast in-memory processing", timestamp: System.system_time(:millisecond)},
      %{id: 3, message: "Perfect for development", timestamp: System.system_time(:millisecond)}
    ]
    
    Enum.each(messages, fn msg ->
      GenRocks.publish("demo_ets", msg)
      Logger.info("Published: #{msg.message}")
    end)
    
    # Consume messages
    {:ok, _consumer} = GenRocks.start_consumer_group("demo_ets", "demo_consumer", fn message, _context ->
      Logger.info("✅ Consumed from ETS: #{message.value.message}")
      :ok
    end)
    
    Process.sleep(2000)
    GenRocks.stop_topic("demo_ets")
    
    Logger.info("""
    
    ✨ ETS Demo Complete!
    
    Key characteristics:
    • ⚡ Extremely fast (in-memory)
    • 🔄 Data lost on restart (not persistent)
    • 🛠️ Perfect for development/testing
    • 📈 Scales with available RAM
    """)
  end
  
  defp step_2_disk_log_basics do
    Logger.info("""
    
    ┌─ Step 2: DiskLog Adapter Basics ────────────────────────────┐
    │                                                              │
    │ DiskLog provides persistent, crash-safe storage with:       │
    │ • Write-Ahead Log (WAL) semantics                           │
    │ • Automatic crash recovery                                   │
    │ • File rotation and management                               │  
    │ • Production-grade durability                                │
    │                                                              │
    └──────────────────────────────────────────────────────────────┘
    """)
    
    Logger.info("Creating topic with DiskLog adapter...")
    
    # DiskLog with custom configuration
    {:ok, _} = GenRocks.start_topic_with_disk_log("demo_disk", 2,
      log_dir: "./getting_started_logs",
      max_no_files: 5,
      max_no_bytes: 1024 * 1024  # 1MB per file
    )
    
    # Publish some critical messages
    critical_messages = [
      %{type: "order", order_id: "ORD-001", amount: 99.99, status: "confirmed"},
      %{type: "payment", order_id: "ORD-001", method: "credit_card", status: "processed"},  
      %{type: "shipment", order_id: "ORD-001", tracking: "TRK-12345", status: "dispatched"}
    ]
    
    Enum.each(critical_messages, fn msg ->
      GenRocks.publish("demo_disk", msg)
      Logger.info("📝 Persisted: #{msg.type} for #{msg.order_id}")
    end)
    
    Logger.info("Simulating system restart by stopping and restarting topic...")
    GenRocks.stop_topic("demo_disk")
    
    Process.sleep(1000)
    
    # Restart with same configuration - data should be recovered!
    {:ok, _} = GenRocks.start_topic_with_disk_log("demo_disk", 2,
      log_dir: "./getting_started_logs",
      max_no_files: 5,
      max_no_bytes: 1024 * 1024
    )
    
    # Verify recovery with consumer
    {:ok, _recovery_consumer} = GenRocks.start_consumer_group("demo_disk", "recovery_consumer", fn message, _context ->
      msg = message.value
      Logger.info("🔄 Recovered after restart: #{msg.type} for #{msg.order_id}")
      :ok
    end)
    
    Process.sleep(2000)
    GenRocks.stop_topic("demo_disk")
    
    Logger.info("""
    
    ✨ DiskLog Demo Complete!
    
    Key characteristics:
    • 💾 Persistent storage (survives restarts)
    • 🛡️ Write-Ahead Log semantics
    • 🔄 Automatic crash recovery  
    • 📁 Organized file structure
    • 🏭 Production-ready durability
    """)
  end
  
  defp step_3_choosing_adapter do
    Logger.info("""
    
    ┌─ Step 3: Choosing the Right Adapter ────────────────────────┐
    │                                                              │
    │ Decision Matrix:                                             │
    │                                                              │
    │                    │    ETS    │  DiskLog                    │
    │ ──────────────────────────────────────────────────────────── │
    │ Performance        │     ⚡⚡⚡    │    ⚡⚡                     │
    │ Durability         │     ❌     │    ✅                      │
    │ Memory Usage       │     📈     │    📉                      │
    │ Setup Complexity   │     ⚡     │    🔧                      │
    │ Production Ready   │     ❌     │    ✅                      │
    │                                                              │
    └──────────────────────────────────────────────────────────────┘
    """)
    
    use_cases = [
      {"🧪 Development & Testing", "ETS", "Fast iteration, no cleanup needed"},
      {"💸 Financial Transactions", "DiskLog", "Never lose money! Audit trails required"},
      {"📊 Real-time Analytics", "ETS", "Speed matters, source data stored elsewhere"}, 
      {"📦 Order Processing", "DiskLog", "Critical business data, must survive crashes"},
      {"🔄 Caching Layer", "ETS", "Temporary speedup, can rebuild from source"},
      {"📋 Audit Logging", "DiskLog", "Compliance requirement, permanent storage"},
      {"🏎️ High-Frequency Trading", "ETS", "Microseconds matter, acceptable risk"},
      {"💾 Event Sourcing", "DiskLog", "System of record, immutable event log"}
    ]
    
    Logger.info("Use Case Recommendations:")
    
    Enum.each(use_cases, fn {use_case, recommended, reason} ->
      adapter_icon = if recommended == "ETS", do: "⚡", else: "💾"
      Logger.info("  #{use_case}")
      Logger.info("    #{adapter_icon} Recommended: #{recommended}")
      Logger.info("    💡 Reason: #{reason}")
      Logger.info("")
    end)
  end
  
  defp step_4_advanced_patterns do
    Logger.info("""
    
    ┌─ Step 4: Advanced Patterns ─────────────────────────────────┐
    │                                                              │
    │ 🏗️ Hybrid Architecture                                       │
    │   Use multiple adapters in the same system:                 │
    │   • ETS for hot/recent data                                  │
    │   • DiskLog for cold/historical data                        │
    │                                                              │
    │ 🔄 Data Lifecycle Management                                 │
    │   • Start with ETS for development                           │
    │   • Graduate to DiskLog for production                       │  
    │   • Archive old data to external systems                     │
    │                                                              │
    └──────────────────────────────────────────────────────────────┘
    """)
    
    Logger.info("Demonstrating hybrid architecture...")
    
    # Hot data path - ETS for speed
    {:ok, _} = GenRocks.start_topic("hot_data", 4)
    
    # Cold data path - DiskLog for persistence
    {:ok, _} = GenRocks.start_topic_with_disk_log("cold_data", 2,
      log_dir: "./getting_started_logs/cold"
    )
    
    # Simulate data routing
    events = [
      %{priority: "high", data: "Critical alert", route_to: "hot"},
      %{priority: "normal", data: "Regular event", route_to: "hot"},
      %{priority: "low", data: "Audit entry", route_to: "cold"},
      %{priority: "archive", data: "Historical record", route_to: "cold"}
    ]
    
    Enum.each(events, fn event ->
      topic = if event.route_to == "hot", do: "hot_data", else: "cold_data"
      GenRocks.publish(topic, event)
      
      storage_type = if event.route_to == "hot", do: "ETS (fast)", else: "DiskLog (persistent)"
      Logger.info("📍 Routed #{event.priority} priority to #{storage_type}")
    end)
    
    # Set up consumers for both paths
    {:ok, _hot_consumer} = GenRocks.start_consumer_group("hot_data", "hot_processor", fn message, _context ->
      Logger.info("⚡ Fast processing: #{message.value.data}")
      :ok
    end)
    
    {:ok, _cold_consumer} = GenRocks.start_consumer_group("cold_data", "cold_processor", fn message, _context ->  
      Logger.info("💾 Persistent processing: #{message.value.data}")
      :ok
    end)
    
    Process.sleep(3000)
    
    GenRocks.stop_topic("hot_data")
    GenRocks.stop_topic("cold_data")
    
    Logger.info("""
    
    ✨ Advanced Patterns Demo Complete!
    
    Key takeaways:
    • 🎯 Choose adapter based on data characteristics
    • 🏗️ Hybrid approaches provide best of both worlds
    • 🔄 Adapt architecture as requirements evolve
    • 📊 Monitor and optimize based on usage patterns
    """)
  end
  
  @doc """
  Quick start example - minimal code to get up and running.
  """
  def quick_start do
    Logger.info("""
    
    🚀 GenRocks Quick Start
    
    1. In-memory processing (development):
    """)
    
    # 1. Start ETS topic (default)
    {:ok, _} = GenRocks.start_topic("my_topic", 2)
    
    # 2. Publish messages
    GenRocks.publish("my_topic", %{message: "Hello GenRocks!"})
    
    # 3. Consume messages
    {:ok, _consumer} = GenRocks.start_consumer_group("my_topic", "my_consumer", fn message, _context ->
      Logger.info("Received: #{message.value.message}")
      :ok
    end)
    
    Process.sleep(1000)
    GenRocks.stop_topic("my_topic")
    
    Logger.info("""
    
    2. Persistent processing (production):
    """)
    
    # 1. Start DiskLog topic  
    {:ok, _} = GenRocks.start_topic_with_disk_log("persistent_topic", 2,
      log_dir: "./quick_start_logs"
    )
    
    # 2. Publish critical data
    GenRocks.publish("persistent_topic", %{order_id: "12345", amount: 99.99})
    
    # 3. Data survives restarts automatically!
    
    GenRocks.stop_topic("persistent_topic")
    
    Logger.info("""
    
    That's it! Check out the other example modules for more advanced patterns.
    """)
    
    {:ok, :completed}
  end
  
  @doc """
  Configuration examples showing different setups.
  """
  def configuration_examples do
    Logger.info("=== Configuration Examples ===")
    
    configurations = [
      {
        "Development Setup",
        %{storage: :ets, partitions: 2},
        "Fast, no persistence needed"
      },
      {
        "Production High-Throughput", 
        %{
          storage: :disk_log,
          partitions: 8,
          config: [
            log_dir: "./prod_logs",
            max_no_files: 20,
            max_no_bytes: 100 * 1024 * 1024  # 100MB
          ]
        },
        "Handle high volume with large files"
      },
      {
        "Production High-Durability",
        %{
          storage: :disk_log,
          partitions: 4,  
          config: [
            log_dir: "./durable_logs",
            max_no_files: 100,  # Long retention
            max_no_bytes: 10 * 1024 * 1024  # 10MB - frequent rotation
          ]
        },
        "Many small files for granular recovery"
      },
      {
        "Single File Archive",
        %{
          storage: :disk_log,
          partitions: 1,
          config: [
            log_dir: "./archive_logs", 
            log_type: :halt,  # Single growing file
            max_no_bytes: 1024 * 1024 * 1024  # 1GB max
          ]
        },
        "Simple append-only log without rotation"
      }
    ]
    
    Enum.each(configurations, fn {name, config, description} ->
      Logger.info("#{name}:")
      Logger.info("  Description: #{description}")
      Logger.info("  Config: #{inspect(config)}")
      
      # Show how to start topic with this configuration
      case config.storage do
        :ets ->
          Logger.info("  Usage: GenRocks.start_topic(\"topic\", #{config.partitions})")
          
        :disk_log ->
          Logger.info("  Usage: GenRocks.start_topic_with_disk_log(\"topic\", #{config.partitions}, #{inspect(config.config)})")
      end
      
      Logger.info("")
    end)
    
    {:ok, :completed}
  end
  
  @doc """
  Cleans up any files created during examples.
  """
  def cleanup do
    dirs_to_clean = [
      "./getting_started_logs"
    ]
    
    Enum.each(dirs_to_clean, fn dir ->
      if File.exists?(dir) do
        File.rm_rf!(dir)
        Logger.info("Cleaned up #{dir}")
      end
    end)
    
    Logger.info("Cleanup completed!")
    :ok
  end
end