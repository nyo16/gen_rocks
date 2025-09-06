defmodule GenRocks.Storage.DiskLogAdapterTest do
  use ExUnit.Case, async: false  # disk_log operations are not async-safe
  
  alias GenRocks.Storage.DiskLogAdapter
  
  @test_topic "test_topic"
  @test_partition 0
  @test_log_dir "./test_logs"
  
  setup do
    # Clean up any existing test logs
    if File.exists?(@test_log_dir) do
      File.rm_rf!(@test_log_dir)
    end
    
    on_exit(fn ->
      # Clean up test logs after each test
      if File.exists?(@test_log_dir) do
        File.rm_rf!(@test_log_dir)
      end
    end)
    
    :ok
  end
  
  describe "open/1" do
    test "creates storage with default options" do
      config = %{
        topic: @test_topic,
        partition: @test_partition,
        log_dir: @test_log_dir
      }
      
      assert {:ok, storage_ref} = DiskLogAdapter.open(config)
      
      assert storage_ref.topic == @test_topic
      assert storage_ref.partition == @test_partition
      assert storage_ref.log_type == :wrap
      assert storage_ref.max_no_files == 10
      assert storage_ref.max_no_bytes == 10 * 1024 * 1024
      
      # Verify directory structure was created
      expected_dir = Path.join([@test_log_dir, @test_topic, "partition_#{@test_partition}"])
      assert File.exists?(expected_dir)
      
      # Clean up
      DiskLogAdapter.close(storage_ref)
    end
    
    test "creates storage with custom options" do
      config = %{
        topic: @test_topic,
        partition: @test_partition,
        log_dir: @test_log_dir,
        log_type: :halt,
        max_no_files: 5,
        max_no_bytes: 1024 * 1024
      }
      
      assert {:ok, storage_ref} = DiskLogAdapter.open(config)
      
      assert storage_ref.log_type == :halt
      assert storage_ref.max_no_files == 5
      assert storage_ref.max_no_bytes == 1024 * 1024
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "creates nested directory structure correctly" do
      topic = "complex/topic/name"
      config = %{
        topic: topic,
        partition: 5,
        log_dir: @test_log_dir
      }
      
      assert {:ok, storage_ref} = DiskLogAdapter.open(config)
      
      expected_dir = Path.join([@test_log_dir, topic, "partition_5"])
      assert File.exists?(expected_dir)
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "returns error when topic is missing" do
      config = %{
        partition: @test_partition,
        log_dir: @test_log_dir
      }
      
      assert {:error, :missing_topic} = DiskLogAdapter.open(config)
    end
  end
  
  describe "put/4 and get/3" do
    setup do
      config = %{
        topic: @test_topic,
        partition: @test_partition,
        log_dir: @test_log_dir
      }
      {:ok, storage_ref} = DiskLogAdapter.open(config)
      %{storage_ref: storage_ref}
    end
    
    test "stores and retrieves simple values", %{storage_ref: storage_ref} do
      key = "test_key"
      value = "test_value"
      
      assert :ok = DiskLogAdapter.put(storage_ref, key, value)
      assert {:ok, ^value} = DiskLogAdapter.get(storage_ref, key)
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "stores and retrieves complex data structures", %{storage_ref: storage_ref} do
      key = "complex_key"
      value = %{
        id: 123,
        name: "Test User",
        nested: %{
          items: [1, 2, 3],
          metadata: %{timestamp: System.system_time()}
        }
      }
      
      assert :ok = DiskLogAdapter.put(storage_ref, key, value)
      assert {:ok, ^value} = DiskLogAdapter.get(storage_ref, key)
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "returns :not_found for non-existent key", %{storage_ref: storage_ref} do
      assert :not_found = DiskLogAdapter.get(storage_ref, "non_existent_key")
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "overwrites existing keys (latest value wins)", %{storage_ref: storage_ref} do
      key = "overwrite_key"
      
      assert :ok = DiskLogAdapter.put(storage_ref, key, "value1")
      assert :ok = DiskLogAdapter.put(storage_ref, key, "value2")
      assert :ok = DiskLogAdapter.put(storage_ref, key, "value3")
      
      assert {:ok, "value3"} = DiskLogAdapter.get(storage_ref, key)
      
      DiskLogAdapter.close(storage_ref)
    end
  end
  
  describe "delete/3 and exists?/3" do
    setup do
      config = %{
        topic: @test_topic,
        partition: @test_partition,
        log_dir: @test_log_dir
      }
      {:ok, storage_ref} = DiskLogAdapter.open(config)
      %{storage_ref: storage_ref}
    end
    
    test "deletes existing key using tombstone", %{storage_ref: storage_ref} do
      key = "delete_key"
      value = "delete_value"
      
      # Store a value
      assert :ok = DiskLogAdapter.put(storage_ref, key, value)
      assert {:ok, ^value} = DiskLogAdapter.get(storage_ref, key)
      assert true = DiskLogAdapter.exists?(storage_ref, key)
      
      # Delete it
      assert :ok = DiskLogAdapter.delete(storage_ref, key)
      assert :not_found = DiskLogAdapter.get(storage_ref, key)
      assert false == DiskLogAdapter.exists?(storage_ref, key)
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "delete followed by put restores value", %{storage_ref: storage_ref} do
      key = "restore_key"
      
      assert :ok = DiskLogAdapter.put(storage_ref, key, "original")
      assert :ok = DiskLogAdapter.delete(storage_ref, key)
      assert :not_found = DiskLogAdapter.get(storage_ref, key)
      assert false == DiskLogAdapter.exists?(storage_ref, key)
      
      assert :ok = DiskLogAdapter.put(storage_ref, key, "restored")
      assert {:ok, "restored"} = DiskLogAdapter.get(storage_ref, key)
      assert true = DiskLogAdapter.exists?(storage_ref, key)
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "exists? returns false for non-existent key", %{storage_ref: storage_ref} do
      assert false == DiskLogAdapter.exists?(storage_ref, "non_existent")
      
      DiskLogAdapter.close(storage_ref)
    end
  end
  
  describe "batch/3" do
    setup do
      config = %{
        topic: @test_topic,
        partition: @test_partition,
        log_dir: @test_log_dir
      }
      {:ok, storage_ref} = DiskLogAdapter.open(config)
      %{storage_ref: storage_ref}
    end
    
    test "performs batch put operations", %{storage_ref: storage_ref} do
      operations = [
        {:put, "key1", "value1"},
        {:put, "key2", "value2"}, 
        {:put, "key3", "value3"}
      ]
      
      assert :ok = DiskLogAdapter.batch(storage_ref, operations)
      
      assert {:ok, "value1"} = DiskLogAdapter.get(storage_ref, "key1")
      assert {:ok, "value2"} = DiskLogAdapter.get(storage_ref, "key2")
      assert {:ok, "value3"} = DiskLogAdapter.get(storage_ref, "key3")
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "performs batch delete operations", %{storage_ref: storage_ref} do
      # First add some data
      assert :ok = DiskLogAdapter.put(storage_ref, "key1", "value1")
      assert :ok = DiskLogAdapter.put(storage_ref, "key2", "value2")
      
      # Then batch delete
      operations = [
        {:delete, "key1"},
        {:delete, "key2"}
      ]
      
      assert :ok = DiskLogAdapter.batch(storage_ref, operations)
      
      assert :not_found = DiskLogAdapter.get(storage_ref, "key1")
      assert :not_found = DiskLogAdapter.get(storage_ref, "key2")
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "performs mixed batch operations", %{storage_ref: storage_ref} do
      # Setup initial state
      assert :ok = DiskLogAdapter.put(storage_ref, "existing", "old_value")
      
      operations = [
        {:put, "new_key", "new_value"},
        {:delete, "existing"},
        {:put, "another", "another_value"}
      ]
      
      assert :ok = DiskLogAdapter.batch(storage_ref, operations)
      
      assert {:ok, "new_value"} = DiskLogAdapter.get(storage_ref, "new_key")
      assert :not_found = DiskLogAdapter.get(storage_ref, "existing")
      assert {:ok, "another_value"} = DiskLogAdapter.get(storage_ref, "another")
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "handles empty batch", %{storage_ref: storage_ref} do
      assert :ok = DiskLogAdapter.batch(storage_ref, [])
      
      DiskLogAdapter.close(storage_ref)
    end
  end
  
  describe "scan/3" do
    setup do
      config = %{
        topic: @test_topic,
        partition: @test_partition,
        log_dir: @test_log_dir
      }
      {:ok, storage_ref} = DiskLogAdapter.open(config)
      
      # Populate with test data
      operations = [
        {:put, "apple", "red"},
        {:put, "banana", "yellow"},
        {:put, "cherry", "red"},
        {:put, "date", "brown"}
      ]
      DiskLogAdapter.batch(storage_ref, operations)
      
      %{storage_ref: storage_ref}
    end
    
    test "scans all entries", %{storage_ref: storage_ref} do
      assert {:ok, entries} = DiskLogAdapter.scan(storage_ref, nil)
      
      # Convert to map for easier testing
      entries_map = Map.new(entries)
      
      assert Map.get(entries_map, "apple") == "red"
      assert Map.get(entries_map, "banana") == "yellow"
      assert Map.get(entries_map, "cherry") == "red"
      assert Map.get(entries_map, "date") == "brown"
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "scans with prefix filter", %{storage_ref: storage_ref} do
      # Add some entries with common prefix
      operations = [
        {:put, "user_1", "john"},
        {:put, "user_2", "jane"},
        {:put, "admin_1", "root"}
      ]
      DiskLogAdapter.batch(storage_ref, operations)
      
      assert {:ok, entries} = DiskLogAdapter.scan(storage_ref, "user_")
      entries_map = Map.new(entries)
      
      # Should only include entries with "user_" prefix
      assert Map.get(entries_map, "user_1") == "john"
      assert Map.get(entries_map, "user_2") == "jane"
      assert Map.get(entries_map, "admin_1") == nil
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "respects limit option", %{storage_ref: storage_ref} do
      assert {:ok, entries} = DiskLogAdapter.scan(storage_ref, nil, limit: 2)
      
      assert length(entries) == 2
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "handles scan with deletes correctly", %{storage_ref: storage_ref} do
      # Delete one entry
      assert :ok = DiskLogAdapter.delete(storage_ref, "banana")
      
      assert {:ok, entries} = DiskLogAdapter.scan(storage_ref, nil)
      entries_map = Map.new(entries)
      
      # Should not include deleted key
      assert Map.get(entries_map, "banana") == nil
      assert Map.get(entries_map, "apple") == "red"
      assert Map.get(entries_map, "cherry") == "red"
      assert Map.get(entries_map, "date") == "brown"
      
      DiskLogAdapter.close(storage_ref)
    end
  end
  
  describe "WAL-specific operations" do
    setup do
      config = %{
        topic: @test_topic,
        partition: @test_partition,
        log_dir: @test_log_dir
      }
      {:ok, storage_ref} = DiskLogAdapter.open(config)
      %{storage_ref: storage_ref}
    end
    
    test "read_log_entries/2 returns entries in chronological order", %{storage_ref: storage_ref} do
      # Add some entries
      assert :ok = DiskLogAdapter.put(storage_ref, "key1", "value1")
      assert :ok = DiskLogAdapter.put(storage_ref, "key2", "value2")
      assert :ok = DiskLogAdapter.delete(storage_ref, "key1")
      assert :ok = DiskLogAdapter.put(storage_ref, "key3", "value3")
      
      assert {:ok, entries} = DiskLogAdapter.read_log_entries(storage_ref)
      
      # Should have 4 entries (3 puts + 1 delete/tombstone)
      assert length(entries) == 4
      
      # Check the order and structure
      [{key1, value1, ts1}, {key2, value2, ts2}, {key1_del, tombstone, ts3}, {key3, value3, ts4}] = entries
      
      assert key1 == "key1"
      assert value1 == "value1"
      assert key2 == "key2"
      assert value2 == "value2"
      assert key1_del == "key1"
      assert tombstone == :__tombstone__
      assert key3 == "key3"
      assert value3 == "value3"
      
      # Timestamps should be in order
      assert ts1 <= ts2
      assert ts2 <= ts3
      assert ts3 <= ts4
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "sync/1 ensures data is written to disk", %{storage_ref: storage_ref} do
      assert :ok = DiskLogAdapter.put(storage_ref, "sync_key", "sync_value")
      assert :ok = DiskLogAdapter.sync(storage_ref)
      
      # After sync, data should be retrievable
      assert {:ok, "sync_value"} = DiskLogAdapter.get(storage_ref, "sync_key")
      
      DiskLogAdapter.close(storage_ref)
    end
    
    test "truncate/1 removes all entries", %{storage_ref: storage_ref} do
      # Add some data
      operations = [
        {:put, "a", "1"},
        {:put, "b", "2"}
      ]
      assert :ok = DiskLogAdapter.batch(storage_ref, operations)
      assert {:ok, "1"} = DiskLogAdapter.get(storage_ref, "a")
      
      # Truncate
      assert :ok = DiskLogAdapter.truncate(storage_ref)
      
      # Data should be gone
      assert :not_found = DiskLogAdapter.get(storage_ref, "a")
      assert :not_found = DiskLogAdapter.get(storage_ref, "b")
      
      assert {:ok, entries} = DiskLogAdapter.read_log_entries(storage_ref)
      assert entries == []
      
      DiskLogAdapter.close(storage_ref)
    end
  end
  
  describe "info/1" do
    setup do
      config = %{
        topic: @test_topic,
        partition: @test_partition,
        log_dir: @test_log_dir
      }
      {:ok, storage_ref} = DiskLogAdapter.open(config)
      %{storage_ref: storage_ref}
    end
    
    test "returns adapter information", %{storage_ref: storage_ref} do
      assert {:ok, info} = DiskLogAdapter.info(storage_ref)
      
      assert info.adapter == "disk_log"
      assert info.topic == @test_topic
      assert info.partition == @test_partition
      assert is_binary(info.log_dir)
      assert info.log_type == :wrap
      assert is_map(info.disk_log_info)
      
      DiskLogAdapter.close(storage_ref)
    end
  end
  
  describe "durability and recovery" do
    @durability_test_log_dir "./durability_test_logs"
    
    setup do
      # Use separate directory for durability tests
      # Clean up any existing durability test logs at start
      if File.exists?(@durability_test_log_dir) do
        File.rm_rf!(@durability_test_log_dir)
      end
      
      # Only clean up at the very end  
      on_exit(fn ->
        if File.exists?(@durability_test_log_dir) do
          File.rm_rf!(@durability_test_log_dir)
        end
      end)
      
      :ok
    end
    
    test "data survives adapter restart" do
      config = %{
        topic: @test_topic,
        partition: @test_partition,
        log_dir: @durability_test_log_dir
      }
      
      # Create initial storage and add data
      {:ok, storage_ref1} = DiskLogAdapter.open(config)
      
      assert :ok = DiskLogAdapter.put(storage_ref1, "persistent_key", "persistent_value")
      assert :ok = DiskLogAdapter.sync(storage_ref1)
      assert :ok = DiskLogAdapter.close(storage_ref1)
      
      # Restart with same configuration
      {:ok, storage_ref2} = DiskLogAdapter.open(config)
      
      # Data should still be there
      assert {:ok, "persistent_value"} = DiskLogAdapter.get(storage_ref2, "persistent_key")
      
      DiskLogAdapter.close(storage_ref2)
    end
    
    test "handles complex WAL replay correctly" do
      config = %{
        topic: @test_topic,
        partition: @test_partition,
        log_dir: @durability_test_log_dir
      }
      
      # Create initial storage
      {:ok, storage_ref1} = DiskLogAdapter.open(config)
      
      # Perform complex operations
      assert :ok = DiskLogAdapter.put(storage_ref1, "key1", "v1")
      assert :ok = DiskLogAdapter.put(storage_ref1, "key2", "v2")
      assert :ok = DiskLogAdapter.put(storage_ref1, "key1", "v1_updated")  # Overwrite
      assert :ok = DiskLogAdapter.delete(storage_ref1, "key2")  # Delete
      assert :ok = DiskLogAdapter.put(storage_ref1, "key3", "v3")
      assert :ok = DiskLogAdapter.put(storage_ref1, "key2", "v2_restored")  # Restore after delete
      
      assert :ok = DiskLogAdapter.sync(storage_ref1)
      assert :ok = DiskLogAdapter.close(storage_ref1)
      
      # Restart and verify final state
      {:ok, storage_ref2} = DiskLogAdapter.open(config)
      
      assert {:ok, "v1_updated"} = DiskLogAdapter.get(storage_ref2, "key1")
      assert {:ok, "v2_restored"} = DiskLogAdapter.get(storage_ref2, "key2") 
      assert {:ok, "v3"} = DiskLogAdapter.get(storage_ref2, "key3")
      
      # Check scan results
      assert {:ok, entries} = DiskLogAdapter.scan(storage_ref2, nil)
      entries_map = Map.new(entries)
      
      assert Map.get(entries_map, "key1") == "v1_updated"
      assert Map.get(entries_map, "key2") == "v2_restored"
      assert Map.get(entries_map, "key3") == "v3"
      
      DiskLogAdapter.close(storage_ref2)
    end
  end
  
  describe "performance characteristics" do
    setup do
      config = %{
        topic: @test_topic,
        partition: @test_partition,
        log_dir: @test_log_dir
      }
      {:ok, storage_ref} = DiskLogAdapter.open(config)
      %{storage_ref: storage_ref}
    end
    
    @tag :performance
    test "handles large number of operations efficiently", %{storage_ref: storage_ref} do
      # Generate test data
      num_operations = 1000
      operations = for i <- 1..num_operations do
        {:put, "key_#{i}", "value_#{i}_" <> String.duplicate("x", 100)}
      end
      
      # Batch insert should be fast
      {batch_time_us, :ok} = :timer.tc(fn ->
        DiskLogAdapter.batch(storage_ref, operations)
      end)
      
      # Should be reasonable performance (less than 1 second for 1000 ops)
      assert batch_time_us < 1_000_000
      
      # Random reads should be fast enough
      {read_time_us, _} = :timer.tc(fn ->
        for i <- 1..100 do
          key = "key_#{:rand.uniform(num_operations)}"
          DiskLogAdapter.get(storage_ref, key)
        end
      end)
      
      # 100 random reads should be under 100ms
      assert read_time_us < 100_000
      
      DiskLogAdapter.close(storage_ref)
    end
    
    @tag :performance
    test "sync operation completes in reasonable time", %{storage_ref: storage_ref} do
      # Add some data
      operations = for i <- 1..100, do: {:put, "sync_key_#{i}", "sync_value_#{i}"}
      DiskLogAdapter.batch(storage_ref, operations)
      
      # Sync should be fast
      {sync_time_us, :ok} = :timer.tc(fn ->
        DiskLogAdapter.sync(storage_ref)
      end)
      
      # Should be under 10ms for small dataset
      assert sync_time_us < 10_000
      
      DiskLogAdapter.close(storage_ref)
    end
  end
end