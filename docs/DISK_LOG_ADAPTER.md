# GenRocks Disk Log Storage Adapter

The disk_log storage adapter provides durable, high-performance storage for GenRocks queues using Erlang's built-in disk_log module. It implements Write-Ahead Log (WAL) semantics with automatic crash recovery, file rotation, and efficient chunk-based reading.

## Overview

The disk_log adapter is designed for production environments where data durability and crash recovery are critical. Unlike the ETS adapter which stores data in memory, the disk_log adapter persists all data to disk immediately, ensuring no data loss even in the event of system crashes.

### Key Features

- **Write-Ahead Logging**: All operations are logged to disk before being acknowledged
- **Crash Recovery**: Automatic replay of log entries on restart
- **File Rotation**: Configurable wrap-around log files prevent unbounded disk usage
- **High Performance**: Optimized batch operations and chunk-based reading
- **Tombstone Deletes**: Proper handling of deletions in append-only logs
- **Latest-Wins Semantics**: Consistent conflict resolution for duplicate keys

## Architecture

### Directory Structure

The disk_log adapter creates a hierarchical directory structure:

```
log_dir/
├── topic_1/
│   ├── partition_0/
│   │   ├── log          # Primary log file
│   │   ├── log.idx      # Index file
│   │   └── log.siz      # Size tracking file
│   ├── partition_1/
│   │   ├── log
│   │   ├── log.idx
│   │   └── log.siz
│   └── ...
├── topic_2/
│   └── ...
└── ...
```

### Log Format

Each log entry is stored as a tuple:
```elixir
{key, value, timestamp}
```

For deletions, a special tombstone value is used:
```elixir
{key, :__tombstone__, timestamp}
```

## Configuration Options

When starting a topic with disk_log storage, you can configure:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:log_dir` | `String.t()` | `"./gen_rocks_data"` | Base directory for log files |
| `:log_type` | `:wrap \| :halt` | `:wrap` | Type of disk_log (wrap for rotation) |
| `:max_no_files` | `pos_integer()` | `10` | Maximum number of wrap files |
| `:max_no_bytes` | `pos_integer()` | `10_485_760` (10MB) | Maximum bytes per wrap file |
| `:repair` | `:truncate \| false` | `:truncate` | Repair mode for corrupted logs |
| `:format` | `:internal \| :external` | `:internal` | Log format (internal recommended) |

## Usage

### Basic Usage

```elixir
# Start a topic with disk_log storage
{:ok, _} = GenRocks.start_topic_with_disk_log("orders", 4)

# Publish messages (they're automatically persisted)
GenRocks.publish("orders", %{order_id: "ORDER-001", amount: 99.99})
```

### Advanced Configuration

```elixir
# Start with custom configuration
{:ok, _} = GenRocks.start_topic_with_disk_log("events", 8,
  log_dir: "/var/log/genrocks",
  max_no_files: 20,
  max_no_bytes: 50 * 1024 * 1024  # 50MB per file
)
```

### Manual Configuration

For more control, you can configure the storage adapter directly:

```elixir
{:ok, _} = GenRocks.start_topic("custom_topic", 2,
  storage_adapter: GenRocks.Storage.DiskLogAdapter,
  storage_config: %{
    log_dir: "./custom_logs",
    log_type: :halt,  # Single file, no rotation
    max_no_bytes: 100 * 1024 * 1024,  # 100MB
    repair: false  # Don't auto-repair corrupted logs
  }
)
```

## Write-Ahead Log Semantics

The disk_log adapter implements proper WAL semantics:

### Durability

All write operations are immediately persisted to disk:
- `put/4` - Appends entry to log
- `delete/3` - Appends tombstone to log  
- `batch/3` - Appends all operations atomically

### Crash Recovery

On restart, the adapter replays the entire log to rebuild the current state:
1. Read all log entries in chronological order
2. Apply "latest wins" semantics for duplicate keys
3. Remove keys that have tombstone entries
4. Build final key-value state

### Example Recovery Scenario

Given this log sequence:
```elixir
{:put, "key1", "value1", 1000}
{:put, "key2", "value2", 1001}  
{:delete, "key1", :__tombstone__, 1002}
{:put, "key1", "new_value", 1003}
```

After recovery, the state will be:
```elixir
%{
  "key1" => "new_value",  # Latest value after delete
  "key2" => "value2"      # Original value
}
```

## Performance Characteristics

### Write Performance

- **Sequential writes**: Optimized for high-throughput sequential writes
- **Batch operations**: Multiple operations logged together for efficiency
- **No read overhead**: Writes don't require reading existing data

### Read Performance

- **Chunk-based reading**: Reads data in 64KB chunks for efficiency
- **Full log replay**: Reads entire log on startup (slower for large logs)
- **Caching recommended**: Consider implementing caching layer for read-heavy workloads

### Memory Usage

- **Minimal memory footprint**: Only metadata kept in memory
- **Bounded memory**: Memory usage independent of data size
- **No data caching**: All data stored on disk

## File Rotation

The disk_log adapter uses wrap logs for automatic file rotation:

### How It Works

1. **Fill current file**: Write to `log.1` until it reaches `max_no_bytes`
2. **Rotate to next**: Start writing to `log.2`
3. **Wrap around**: After `max_no_files`, wrap back to `log.1`
4. **Overwrite old data**: Oldest data is overwritten when wrapping

### Configuration Trade-offs

| Configuration | Pros | Cons |
|---------------|------|------|
| Many small files | Better crash recovery, less data loss | More file operations, slower |
| Few large files | Better performance, less overhead | Potential for more data loss |

## Operational Considerations

### Monitoring

Monitor these metrics in production:
- **Disk space usage**: Ensure adequate space for log rotation
- **File count**: Verify rotation is working properly
- **Write latency**: Monitor for performance degradation
- **Recovery time**: Track startup time for large logs

### Backup and Archival

```bash
# Create backup of topic logs
tar -czf orders-backup-$(date +%Y%m%d).tar.gz ./gen_rocks_data/orders/

# Archive old partitions
mv ./gen_rocks_data/orders/partition_0 ./archive/orders-p0-$(date +%Y%m%d)
```

### Disaster Recovery

1. **Regular backups**: Backup log directories regularly
2. **Replication**: Consider file-level replication for critical data
3. **Recovery testing**: Regularly test recovery procedures

## Troubleshooting

### Common Issues

#### Log Corruption
```elixir
# Error: {:disk_log_error, {:error, {read_error, {file_error, ...}}}}
```

**Solution**: Enable repair mode or restore from backup:
```elixir
{:ok, _} = GenRocks.start_topic_with_disk_log("topic", 1,
  repair: :truncate  # Auto-repair corrupted logs
)
```

#### Disk Space Issues
```elixir
# Error: {:disk_log_error, {:error, {write_error, enospc}}}
```

**Solution**: 
- Free disk space
- Reduce `max_no_files` or `max_no_bytes`
- Move to larger disk

#### Slow Recovery
Long startup times with large logs.

**Solution**:
- Reduce log size through more aggressive rotation
- Consider periodic compaction
- Use SSD storage for better I/O performance

### Debug Tools

```elixir
# Get adapter info
{:ok, info} = GenRocks.Storage.DiskLogAdapter.info(storage_ref)

# Read raw log entries
{:ok, entries} = GenRocks.Storage.DiskLogAdapter.read_log_entries(storage_ref)

# Truncate logs (destructive!)
GenRocks.Storage.DiskLogAdapter.truncate(storage_ref)
```

## Best Practices

### Production Deployment

1. **Use dedicated disk**: Store logs on separate disk from application
2. **Monitor disk space**: Set up alerts for disk usage
3. **Regular backups**: Implement automated backup procedures
4. **Test recovery**: Regularly test disaster recovery procedures

### Performance Optimization

1. **Batch operations**: Use batch operations when possible
2. **Appropriate file sizes**: Balance between performance and durability
3. **SSD storage**: Use SSDs for better I/O performance
4. **Partition strategy**: Distribute load across multiple partitions

### Development and Testing

```elixir
# Use small files for faster testing
{:ok, _} = GenRocks.start_topic_with_disk_log("test_topic", 1,
  log_dir: "./test_logs",
  max_no_files: 3,
  max_no_bytes: 1024  # 1KB files
)

# Clean up test data
GenRocks.Examples.DiskLogExamples.cleanup_examples()
```

## Comparison with Other Adapters

| Feature | ETS Adapter | Disk Log Adapter | RocksDB Adapter |
|---------|-------------|------------------|-----------------|
| **Durability** | ❌ Memory only | ✅ Persistent | ✅ Persistent |
| **Performance** | ⚡ Fastest | 🚀 Fast | 🏃 Good |
| **Memory Usage** | 📈 High | 📉 Low | 📊 Medium |
| **Crash Recovery** | ❌ No | ✅ Yes | ✅ Yes |
| **Setup Complexity** | ⚡ Simple | 🚀 Easy | 🔧 Complex |
| **Production Ready** | ❌ Dev only | ✅ Yes | ✅ Yes |

## Examples

See `GenRocks.Examples.DiskLogExamples` for comprehensive usage examples:

```elixir
# Run all examples
GenRocks.Examples.DiskLogExamples.run_all_examples()

# Run specific examples
GenRocks.Examples.DiskLogExamples.crash_recovery_example()
GenRocks.Examples.DiskLogExamples.high_throughput_example()
```

## Implementation Details

The disk_log adapter is built on Erlang's `disk_log` module, which provides:
- **Atomic operations**: All writes are atomic at the OS level  
- **Efficient I/O**: Optimized for sequential write patterns
- **Built-in rotation**: Native support for wrap logs
- **Error recovery**: Automatic repair of corrupted logs
- **Cross-platform**: Works on all Erlang-supported platforms

For more information about the underlying `disk_log` module, see:
[Erlang disk_log documentation](https://www.erlang.org/doc/apps/kernel/disk_log.html)