# GenRocks Storage Adapters Guide

GenRocks provides a pluggable storage architecture that allows you to choose the right storage backend for your specific use case. This guide covers all available storage adapters, their characteristics, and how to choose and use them effectively.

## Available Storage Adapters

### 📦 ETS Adapter (Default)
**Best for:** Development, testing, high-performance scenarios where data loss is acceptable

```elixir
# Start topic with ETS adapter (default)
{:ok, _} = GenRocks.start_topic("fast_topic", 4)

# Publish messages - stored in memory
GenRocks.publish("fast_topic", %{data: "Fast processing"})
```

**Characteristics:**
- ⚡ **Extremely fast** - Sub-millisecond latency
- 🔄 **Not persistent** - Data lost on restart  
- 📈 **Memory-bound** - Limited by available RAM
- 🛠️ **Zero setup** - No configuration needed
- 🧪 **Perfect for development** - Fast iteration cycles

**Use Cases:**
- Development and testing environments
- High-frequency trading systems
- Caching layers
- Temporary data processing
- Real-time analytics where source data is stored elsewhere

### 💾 DiskLog Adapter 
**Best for:** Production systems requiring durability and crash recovery

```elixir
# Start topic with DiskLog adapter
{:ok, _} = GenRocks.start_topic_with_disk_log("orders", 4,
  log_dir: "./production_logs",
  max_no_files: 20,
  max_no_bytes: 100 * 1024 * 1024  # 100MB per file
)

# Publish messages - automatically persisted to disk
GenRocks.publish("orders", %{order_id: "12345", amount: 99.99})
```

**Characteristics:**
- 💾 **Fully persistent** - Survives system crashes and restarts
- 🛡️ **Write-Ahead Log (WAL) semantics** - ACID-like properties
- 🔄 **Automatic recovery** - Replays log on startup
- 📁 **Hierarchical organization** - `log_dir/topic/partition_N` structure
- 🔧 **Configurable rotation** - Automatic file management
- 📊 **Lower memory usage** - Data stored on disk

**Use Cases:**
- Production message queues
- Financial transaction processing
- Event sourcing systems
- Audit logging
- Any system where data loss is unacceptable

## Configuration Options

### ETS Adapter
ETS adapter requires no configuration - it's ready to use out of the box:

```elixir
# These are equivalent
{:ok, _} = GenRocks.start_topic("topic", 4)
{:ok, _} = GenRocks.start_topic("topic", 4, storage_adapter: GenRocks.Storage.EtsAdapter)
```

### DiskLog Adapter
DiskLog adapter offers extensive configuration options:

```elixir
{:ok, _} = GenRocks.start_topic_with_disk_log("topic", 4,
  # Base directory for log files
  log_dir: "./my_logs",
  
  # Log rotation settings
  log_type: :wrap,           # :wrap (rotation) or :halt (single file)
  max_no_files: 10,          # Number of files in rotation
  max_no_bytes: 50 * 1024 * 1024,  # 50MB per file
  
  # Recovery settings  
  repair: true,              # Auto-repair corrupted logs
  format: :internal          # Erlang internal format (recommended)
)
```

#### Configuration Presets

**Development Setup:**
```elixir
{:ok, _} = GenRocks.start_topic_with_disk_log("dev_topic", 2,
  log_dir: "./dev_logs",
  max_no_files: 3,
  max_no_bytes: 1024 * 1024  # 1MB files for quick testing
)
```

**Production High-Throughput:**
```elixir
{:ok, _} = GenRocks.start_topic_with_disk_log("high_throughput", 8,
  log_dir: "/var/log/genrocks/high_throughput",
  max_no_files: 5,
  max_no_bytes: 500 * 1024 * 1024  # 500MB files, less rotation overhead
)
```

**Production High-Durability:**
```elixir
{:ok, _} = GenRocks.start_topic_with_disk_log("critical_data", 4,
  log_dir: "/var/log/genrocks/critical",
  max_no_files: 100,          # Long retention period
  max_no_bytes: 10 * 1024 * 1024  # 10MB files, frequent rotation
)
```

**Archive/Compliance:**
```elixir
{:ok, _} = GenRocks.start_topic_with_disk_log("audit_log", 1,
  log_dir: "/var/log/genrocks/audit", 
  log_type: :halt,            # Single growing file
  max_no_bytes: 1024 * 1024 * 1024  # 1GB maximum size
)
```

## Adapter Selection Matrix

| Criteria | ETS | DiskLog | Notes |
|----------|-----|---------|-------|
| **Performance** | ⚡⚡⚡ | ⚡⚡ | ETS ~10x faster for pure speed |
| **Durability** | ❌ | ✅ | DiskLog survives crashes |
| **Memory Usage** | 📈 High | 📉 Low | ETS stores all data in RAM |
| **Setup Complexity** | ⚡ None | 🔧 Moderate | DiskLog needs directory config |
| **Production Ready** | ❌ | ✅ | ETS loses data on restart |
| **Scalability** | RAM-limited | Disk-limited | Choose based on your bottleneck |
| **Debugging** | Easy | Moderate | ETS data visible in observer |

## Hybrid Architectures

You can combine multiple adapters in the same system for optimal performance and durability:

### Hot/Cold Data Pattern
```elixir
# Hot data - recent, frequently accessed (ETS)
{:ok, _} = GenRocks.start_topic("hot_events", 8)  # High partition count

# Cold data - historical, infrequently accessed (DiskLog) 
{:ok, _} = GenRocks.start_topic_with_disk_log("cold_events", 2,
  log_dir: "./cold_storage",
  max_no_files: 50,  # Long retention
  max_no_bytes: 100 * 1024 * 1024
)

# Route based on data age or access pattern
route_event = fn event ->
  if recent?(event) do
    GenRocks.publish("hot_events", event)
  else
    GenRocks.publish("cold_events", event)
  end
end
```

### Tiered Processing
```elixir
# L1: Real-time processing (ETS)
{:ok, _} = GenRocks.start_topic("realtime_processing", 16)

# L2: Batch processing (DiskLog)  
{:ok, _} = GenRocks.start_topic_with_disk_log("batch_processing", 4,
  log_dir: "./batch_storage"
)

# L3: Long-term storage (DiskLog with archival settings)
{:ok, _} = GenRocks.start_topic_with_disk_log("archival", 1,
  log_dir: "./archives",
  log_type: :halt,
  max_no_bytes: 10 * 1024 * 1024 * 1024  # 10GB archives
)
```

## Migration Strategies

### Development to Production
1. **Start with ETS** for fast development cycles
2. **Export checkpoints** before migration
3. **Switch to DiskLog** with appropriate configuration  
4. **Import checkpoints** to new persistent storage
5. **Verify data integrity** and performance

```elixir
# Phase 1: Development (ETS)
{:ok, _} = GenRocks.start_topic("user_events", 2)

# ... development and testing ...

# Phase 2: Pre-migration checkpoint
GenRocks.start_consumer_group("user_events", "checkpoint_exporter", fn msg, _ctx ->
  # Export to external system, file, or database
  export_to_persistent_store(msg)
  :ok
end)

# Phase 3: Production migration (DiskLog)
GenRocks.stop_topic("user_events")
{:ok, _} = GenRocks.start_topic_with_disk_log("user_events", 2,
  log_dir: "./production_events"
)

# Phase 4: Import checkpointed data
import_from_persistent_store("user_events")
```

## Best Practices

### For ETS Adapter
- ✅ **Monitor memory usage** - Set up alerts for RAM consumption
- ✅ **Use for development** - Perfect for testing and prototyping
- ✅ **Consider as cache** - Great for temporary acceleration
- ❌ **Don't use in production** - Unless data loss is acceptable
- ❌ **Don't store critical data** - No persistence guarantees

### For DiskLog Adapter  
- ✅ **Plan disk space** - Monitor usage and set up rotation
- ✅ **Backup regularly** - Implement backup strategies
- ✅ **Test recovery** - Regularly verify crash recovery works
- ✅ **Tune configuration** - Balance performance vs. durability
- ✅ **Use separate disks** - Separate OS and log storage if possible

### General Guidelines
- 🎯 **Choose based on data characteristics**, not technology preferences
- 🔍 **Measure performance** in your specific environment
- 🧪 **Start simple** and evolve based on requirements
- 📊 **Monitor continuously** and adjust configuration as needed
- 🔄 **Plan for growth** - Consider future scalability needs

## Example Use Cases

### E-commerce System
```elixir
# Shopping carts - temporary, high performance
{:ok, _} = GenRocks.start_topic("shopping_carts", 8)

# Orders - critical, must persist
{:ok, _} = GenRocks.start_topic_with_disk_log("orders", 4,
  log_dir: "./orders", max_no_files: 20, max_no_bytes: 50 * 1024 * 1024)

# Analytics events - high volume, some loss acceptable
{:ok, _} = GenRocks.start_topic("analytics", 16)

# Audit trail - compliance requirement
{:ok, _} = GenRocks.start_topic_with_disk_log("audit", 1,
  log_dir: "./audit", log_type: :halt, max_no_bytes: 1024 * 1024 * 1024)
```

### IoT Data Processing
```elixir
# Sensor readings - high volume, recent data most important
{:ok, _} = GenRocks.start_topic("sensor_readings", 32)

# Device commands - must be delivered
{:ok, _} = GenRocks.start_topic_with_disk_log("device_commands", 8,
  log_dir: "./commands")

# Aggregated metrics - permanent storage for analysis
{:ok, _} = GenRocks.start_topic_with_disk_log("metrics", 4,
  log_dir: "./metrics", max_no_files: 100)
```

## Troubleshooting

### Common ETS Issues
- **OutOfMemory errors** - Reduce data volume or switch to DiskLog
- **Data disappears** - Expected behavior, use DiskLog for persistence
- **Slow startup** - Normal, ETS tables recreated on each restart

### Common DiskLog Issues
- **Disk space errors** - Increase disk space or reduce retention
- **Slow performance** - Tune file sizes and rotation settings
- **Corruption errors** - Enable repair mode or restore from backup
- **Permission errors** - Ensure write access to log directories

## Getting Started

### Quick Start (5 minutes)
1. **Try ETS** - Use default settings for immediate gratification
2. **Try DiskLog** - Test persistence across restarts
3. **Compare performance** - Measure the difference in your environment
4. **Choose adapter** - Based on your specific requirements

### Examples and Code
- `GenRocks.Examples.GettingStarted` - Step-by-step guide with working code
- `GenRocks.Examples.StorageAdapterExamples` - Comprehensive usage patterns  
- `GenRocks.Examples.DataProcessingExamples` - Real-world processing pipelines
- `GenRocks.Examples.DiskLogExamples` - DiskLog-specific advanced features

### Interactive Learning
```elixir
# Run the interactive guide
GenRocks.Examples.GettingStarted.interactive_guide()

# Try specific examples
GenRocks.Examples.StorageAdapterExamples.ets_adapter_example()
GenRocks.Examples.StorageAdapterExamples.disk_log_adapter_example()

# Performance comparison
GenRocks.Examples.StorageAdapterExamples.adapter_performance_comparison()
```

## Conclusion

GenRocks storage adapters provide the flexibility to optimize for your specific requirements:

- **Start with ETS** for development and high-performance scenarios
- **Graduate to DiskLog** when you need persistence and durability  
- **Combine both** in hybrid architectures for optimal performance
- **Monitor and tune** based on your actual usage patterns

The pluggable architecture means you can evolve your storage strategy as your requirements change, without rewriting your application logic.

Happy queueing! 🚀