defmodule GenRocks.Examples.SourcesSinksExamples do
  @moduledoc """
  Examples demonstrating Sources and Sinks functionality.
  Shows various patterns for data ingestion and output.
  """

  require Logger

  @doc """
  Basic file reading example.
  Reads a text file line by line and processes it.
  """
  def basic_file_reading_example do
    Logger.info("Starting basic file reading example...")

    # Create sample text file
    sample_file = "tmp/sample.txt"
    create_sample_text_file(sample_file)

    # Read and process
    result = GenRocks.read_file(sample_file)
    |> GenRocks.filter(fn msg -> 
      not String.starts_with?(msg.value, "#")  # Skip comments
    end)
    |> GenRocks.transform(fn msg ->
      %{msg | value: String.upcase(msg.value)}
    end)
    |> GenRocks.collect()

    Logger.info("Processed #{length(result)} lines from file")
    result
  end

  @doc """
  JSON file processing example.
  Reads JSONL file and transforms the data.
  """
  def json_processing_example do
    Logger.info("Starting JSON processing example...")

    # Create sample JSONL file
    json_file = "tmp/events.jsonl"
    create_sample_json_file(json_file)

    # Process JSON data
    result = GenRocks.read_file(json_file, format: :json)
    |> GenRocks.filter(fn msg ->
      msg.value["type"] == "user_action"
    end)
    |> GenRocks.transform(fn msg ->
      enriched = Map.put(msg.value, "processed_at", DateTime.utc_now())
      %{msg | value: enriched}
    end)
    |> GenRocks.group_by_key(fn msg -> msg.value["user_id"] end)
    |> GenRocks.reduce(
      fn -> %{actions: [], count: 0} end,
      fn {user_id, events}, acc ->
        events_list = if is_list(events), do: events, else: [events]
        %{
          user_id: user_id,
          actions: acc.actions ++ Enum.map(events_list, fn e -> e.value end),
          count: acc.count + length(events_list)
        }
      end
    )
    |> GenRocks.collect()

    Logger.info("Processed user actions from JSON file")
    result
  end

  @doc """
  CSV processing with custom headers.
  """
  def csv_processing_example do
    Logger.info("Starting CSV processing example...")

    # Create sample CSV
    csv_file = "tmp/users.csv"
    create_sample_csv_file(csv_file)

    # Read CSV with custom processing
    result = GenRocks.read_file(csv_file, format: :csv)
    |> GenRocks.transform(fn msg ->
      case msg.value.fields do
        [id, name, email, age] ->
          user_data = %{
            id: String.to_integer(id),
            name: String.trim(name),
            email: String.downcase(String.trim(email)),
            age: String.to_integer(age),
            email_domain: email |> String.split("@") |> List.last()
          }
          %{msg | value: user_data}
        _ ->
          msg
      end
    end)
    |> GenRocks.filter(fn msg ->
      is_map(msg.value) && msg.value.age >= 18
    end)
    |> GenRocks.collect()

    Logger.info("Processed #{length(result)} adult users from CSV")
    result
  end

  @doc """
  File-to-file transformation pipeline.
  Demonstrates reading from one file and writing to another.
  """
  def file_transformation_pipeline do
    Logger.info("Starting file transformation pipeline...")

    input_file = "tmp/input_data.jsonl"
    output_file = "tmp/transformed_data.jsonl"

    # Create input data
    create_sample_transaction_data(input_file)

    # Transform and write
    GenRocks.read_file(input_file, format: :json)
    |> GenRocks.filter(fn msg ->
      msg.value["amount"] > 100  # Only high-value transactions
    end)
    |> GenRocks.transform(fn msg ->
      transformed = %{
        transaction_id: msg.value["id"],
        amount_usd: msg.value["amount"],
        merchant: msg.value["merchant"],
        category: categorize_merchant(msg.value["merchant"]),
        risk_score: calculate_risk_score(msg.value),
        processed_at: DateTime.utc_now()
      }
      %{msg | value: transformed}
    end)
    |> GenRocks.write_file(output_file, format: :json)
    |> Flow.run()

    Logger.info("Transformation pipeline completed: #{input_file} -> #{output_file}")
  end

  @doc """
  Multiple file aggregation example.
  Reads from multiple files using glob patterns.
  """
  def multi_file_aggregation_example do
    Logger.info("Starting multi-file aggregation example...")

    # Create multiple log files
    create_multiple_log_files("tmp/logs/*.log")

    # Aggregate data from all files
    result = GenRocks.from_source(GenRocks.Source.FileSource, %{
      glob_pattern: "tmp/logs/*.log",
      format: :lines,
      topic: "aggregated_logs"
    })
    |> GenRocks.filter(fn msg ->
      String.contains?(msg.value, "ERROR") or String.contains?(msg.value, "WARN")
    end)
    |> GenRocks.transform(fn msg ->
      %{
        level: extract_log_level(msg.value),
        message: msg.value,
        file_source: msg.metadata.file_path,
        line_number: msg.metadata.line_number
      }
    end)
    |> GenRocks.group_by_key(fn msg -> msg.level end)
    |> GenRocks.aggregate(:count)
    |> GenRocks.collect()

    Logger.info("Aggregated logs from multiple files: #{inspect(result)}")
    result
  end

  @doc """
  Custom parser example.
  Uses a custom parser function to process specialized file formats.
  """
  def custom_parser_example do
    Logger.info("Starting custom parser example...")

    config_file = "tmp/app.config"
    create_sample_config_file(config_file)

    # Custom parser for config file format
    config_parser = fn line, _state ->
      trimmed = String.trim(line)
      
      cond do
        String.starts_with?(trimmed, "#") -> :skip  # Skip comments
        String.starts_with?(trimmed, "[") -> :skip  # Skip sections for now
        String.contains?(trimmed, "=") ->
          case String.split(trimmed, "=", parts: 2) do
            [key, value] ->
              {:ok, %{
                key: String.trim(key),
                value: String.trim(value),
                type: determine_config_type(value)
              }}
            _ -> :skip
          end
        true -> :skip
      end
    end

    result = GenRocks.from_source(GenRocks.Source.FileSource, %{
      file_path: config_file,
      format: :custom,
      parser: config_parser,
      topic: "config"
    })
    |> GenRocks.group_by_key(fn msg -> msg.value.type end)
    |> GenRocks.collect()

    Logger.info("Parsed config file with custom parser")
    result
  end

  @doc """
  Real-time processing simulation with sources and sinks.
  """
  def realtime_processing_example do
    Logger.info("Starting real-time processing example...")

    input_topic = "sensor_data"
    output_file = "tmp/processed_sensors.jsonl"

    # Start the topic
    {:ok, _} = GenRocks.start_topic(input_topic, 4)

    # Start sink consumer to write processed data
    {:ok, _sink} = GenRocks.start_sink_consumer(
      GenRocks.Sink.FileSink,
      %{
        file_path: output_file,
        format: :json,
        rotation_size: 10 * 1024  # Small size for demo
      },
      subscribe_to: [{:via, Registry, {GenRocks.ProducerRegistry, input_topic}}],
      name: :sensor_sink
    )

    # Simulate sensor data
    spawn(fn -> simulate_sensor_data(input_topic, 50) end)

    # Start processing consumer
    processor_fn = fn message, _context ->
      # Simulate processing
      processed_data = %{
        sensor_id: message.value.sensor_id,
        reading: message.value.reading,
        normalized_reading: normalize_reading(message.value.reading),
        anomaly_detected: detect_anomaly(message.value.reading),
        processed_at: DateTime.utc_now()
      }

      # Send processed data to sink (via topic)
      GenRocks.publish(input_topic, processed_data, key: "processed_#{message.key}")
      :ok
    end

    {:ok, _group} = GenRocks.start_consumer_group("sensor_processors", input_topic, processor_fn,
      partitions: [0, 1, 2, 3]
    )

    # Let it run for a few seconds
    Process.sleep(5000)

    Logger.info("Real-time processing example completed, check #{output_file}")
  end

  @doc """
  Batch processing with multiple output formats.
  """
  def multi_format_output_example do
    Logger.info("Starting multi-format output example...")

    # Create input data
    input_file = "tmp/sales_data.jsonl"
    create_sample_sales_data(input_file)

    # Read the data once
    base_flow = GenRocks.read_file(input_file, format: :json)
    |> GenRocks.transform(fn msg ->
      %{
        date: msg.value["date"],
        amount: msg.value["amount"],
        region: msg.value["region"],
        product: msg.value["product"]
      }
    end)

    # Output to JSON
    json_output = base_flow
    |> GenRocks.write_file("tmp/sales_summary.json", format: :json)

    # Output to CSV  
    csv_output = base_flow
    |> GenRocks.write_file("tmp/sales_summary.csv", 
      format: :csv,
      csv_headers: ["date", "amount", "region", "product"]
    )

    # Output summary to text
    text_output = base_flow
    |> GenRocks.aggregate(:count)
    |> GenRocks.transform(fn count ->
      "Total sales records processed: #{count}\nGenerated on: #{DateTime.utc_now()}"
    end)
    |> GenRocks.write_file("tmp/sales_summary.txt", format: :lines)

    # Run all outputs
    Task.async(fn -> Flow.run(json_output) end)
    Task.async(fn -> Flow.run(csv_output) end)
    Task.async(fn -> Flow.run(text_output) end)
    |> Task.await_many(10_000)

    Logger.info("Multi-format output completed")
  end

  # Helper functions for data creation and processing

  defp create_sample_text_file(file_path) do
    File.mkdir_p!(Path.dirname(file_path))
    
    content = """
    # Sample text file
    This is line 1
    This is line 2
    # This is a comment
    This is line 3
    Another line here
    # Another comment
    Final line
    """
    
    File.write!(file_path, content)
  end

  defp create_sample_json_file(file_path) do
    File.mkdir_p!(Path.dirname(file_path))
    
    events = [
      %{type: "user_action", user_id: 1, action: "login", timestamp: DateTime.utc_now()},
      %{type: "user_action", user_id: 2, action: "purchase", timestamp: DateTime.utc_now()},
      %{type: "system_event", level: "info", message: "System started"},
      %{type: "user_action", user_id: 1, action: "logout", timestamp: DateTime.utc_now()},
      %{type: "user_action", user_id: 3, action: "signup", timestamp: DateTime.utc_now()}
    ]
    
    content = events
    |> Enum.map(&Jason.encode!/1)
    |> Enum.join("\n")
    
    File.write!(file_path, content)
  end

  defp create_sample_csv_file(file_path) do
    File.mkdir_p!(Path.dirname(file_path))
    
    content = """
    1,John Doe,john@example.com,25
    2,Jane Smith,jane@example.com,17
    3,Bob Johnson,bob@example.com,30
    4,Alice Brown,alice@example.com,22
    5,Charlie Wilson,charlie@example.com,16
    """
    
    File.write!(file_path, content)
  end

  defp create_sample_transaction_data(file_path) do
    File.mkdir_p!(Path.dirname(file_path))
    
    transactions = 1..20
    |> Enum.map(fn i ->
      %{
        id: "txn_#{i}",
        amount: :rand.uniform() * 1000,
        merchant: Enum.random(["Amazon", "Walmart", "Starbucks", "Gas Station", "Restaurant"]),
        timestamp: DateTime.utc_now()
      }
    end)
    
    content = transactions
    |> Enum.map(&Jason.encode!/1)
    |> Enum.join("\n")
    
    File.write!(file_path, content)
  end

  defp create_multiple_log_files(pattern) do
    base_dir = Path.dirname(pattern)
    File.mkdir_p!(base_dir)
    
    log_files = ["app1.log", "app2.log", "system.log"]
    
    Enum.each(log_files, fn filename ->
      file_path = Path.join(base_dir, filename)
      
      log_lines = 1..20
      |> Enum.map(fn i ->
        level = Enum.random(["INFO", "WARN", "ERROR", "DEBUG"])
        "2024-01-01 12:00:#{String.pad_leading("#{i}", 2, "0")} [#{level}] #{filename}: Message #{i}"
      end)
      
      File.write!(file_path, Enum.join(log_lines, "\n"))
    end)
  end

  defp create_sample_config_file(file_path) do
    File.mkdir_p!(Path.dirname(file_path))
    
    content = """
    # Application Configuration
    [database]
    host=localhost
    port=5432
    name=myapp_db
    
    [server]
    host=0.0.0.0
    port=4000
    ssl=true
    
    [features]
    debug=false
    logging=true
    cache_size=1000
    """
    
    File.write!(file_path, content)
  end

  defp create_sample_sales_data(file_path) do
    File.mkdir_p!(Path.dirname(file_path))
    
    sales = 1..15
    |> Enum.map(fn i ->
      %{
        date: Date.utc_today() |> Date.to_iso8601(),
        amount: (:rand.uniform() * 1000) |> Float.round(2),
        region: Enum.random(["North", "South", "East", "West"]),
        product: Enum.random(["Widget A", "Widget B", "Widget C"])
      }
    end)
    
    content = sales
    |> Enum.map(&Jason.encode!/1)
    |> Enum.join("\n")
    
    File.write!(file_path, content)
  end

  defp categorize_merchant(merchant) do
    case merchant do
      "Amazon" -> "retail"
      "Walmart" -> "retail"
      "Starbucks" -> "food"
      "Gas Station" -> "fuel"
      "Restaurant" -> "food"
      _ -> "other"
    end
  end

  defp calculate_risk_score(transaction) do
    # Simple risk scoring
    base_score = if transaction["amount"] > 500, do: 30, else: 10
    time_score = if DateTime.utc_now().hour < 6 or DateTime.utc_now().hour > 23, do: 20, else: 0
    base_score + time_score
  end

  defp extract_log_level(line) do
    cond do
      String.contains?(line, "[ERROR]") -> "ERROR"
      String.contains?(line, "[WARN]") -> "WARN"
      String.contains?(line, "[INFO]") -> "INFO"
      String.contains?(line, "[DEBUG]") -> "DEBUG"
      true -> "UNKNOWN"
    end
  end

  defp determine_config_type(value) do
    cond do
      value in ["true", "false"] -> "boolean"
      String.match?(value, ~r/^\d+$/) -> "integer"
      String.match?(value, ~r/^\d+\.\d+$/) -> "float"
      true -> "string"
    end
  end

  defp simulate_sensor_data(topic, count) do
    1..count
    |> Enum.each(fn i ->
      sensor_data = %{
        sensor_id: "sensor_#{rem(i, 5) + 1}",
        reading: :rand.uniform() * 100,
        timestamp: DateTime.utc_now(),
        location: Enum.random(["A", "B", "C", "D"])
      }
      
      GenRocks.publish(topic, sensor_data, key: "sensor_#{i}")
      Process.sleep(50)  # Simulate real-time data
    end)
  end

  defp normalize_reading(reading) do
    # Simple normalization to 0-1 range
    reading / 100
  end

  defp detect_anomaly(reading) do
    # Simple anomaly detection
    reading > 90 or reading < 10
  end
end