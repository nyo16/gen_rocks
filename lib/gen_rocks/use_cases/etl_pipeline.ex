defmodule GenRocks.UseCases.ETLPipeline do
  @moduledoc """
  Extract, Transform, Load (ETL) pipeline use case using Sources and Sinks.
  Demonstrates file-to-file data processing with various transformations.
  """

  require Logger

  @doc """
  Simple ETL pipeline: CSV to JSON transformation.
  Reads CSV file, transforms data, writes to JSON format.
  """
  def csv_to_json_etl(input_file, output_file) do
    Logger.info("Starting CSV to JSON ETL: #{input_file} -> #{output_file}")

    # Create sample CSV data first
    create_sample_csv(input_file)

    result = GenRocks.read_file(input_file, format: :csv)
    |> GenRocks.transform(fn msg ->
      # Transform CSV fields to structured JSON
      case msg.value.fields do
        [id, name, email, age, department] ->
          json_data = %{
            id: String.to_integer(id),
            name: String.trim(name),
            email: String.downcase(String.trim(email)),
            age: String.to_integer(age),
            department: String.trim(department),
            processed_at: DateTime.utc_now()
          }
          %{msg | value: json_data}
        
        _ ->
          Logger.warning("Invalid CSV row: #{inspect(msg.value)}")
          msg
      end
    end)
    |> GenRocks.filter(fn msg -> 
      # Filter out invalid entries
      is_map(msg.value) && Map.has_key?(msg.value, :id)
    end)
    |> GenRocks.write_file(output_file, format: :json)
    |> Flow.run()

    Logger.info("CSV to JSON ETL completed. Result: #{inspect(result)}")
    result
  end

  @doc """
  Log processing ETL pipeline.
  Reads log files, parses them, filters errors, writes to structured format.
  """
  def log_processing_etl(log_pattern, output_file) do
    Logger.info("Starting log processing ETL: #{log_pattern} -> #{output_file}")

    # Create sample log files
    create_sample_logs(log_pattern)

    # Custom parser for log lines
    log_parser = fn line, _state ->
      case parse_log_line(line) do
        {:ok, parsed} -> {:ok, parsed}
        :error -> :skip  # Skip invalid lines
      end
    end

    GenRocks.from_source(GenRocks.Source.FileSource, %{
      glob_pattern: log_pattern,
      format: :custom,
      parser: log_parser,
      topic: "logs"
    })
    |> GenRocks.filter(fn msg -> 
      # Only process ERROR and WARN level logs
      msg.value.level in ["ERROR", "WARN"]
    end)
    |> GenRocks.transform(fn msg ->
      # Enrich with additional metadata
      enriched_data = Map.merge(msg.value, %{
        processed_at: DateTime.utc_now(),
        severity_score: calculate_severity_score(msg.value.level),
        alert_required: msg.value.level == "ERROR"
      })
      %{msg | value: enriched_data}
    end)
    |> GenRocks.group_by_key(fn msg -> msg.value.application end)
    |> GenRocks.reduce(
      fn -> %{errors: [], warnings: [], total_count: 0} end,
      fn {app, logs}, acc ->
        logs_list = if is_list(logs), do: logs, else: [logs]
        
        errors = Enum.filter(logs_list, fn log -> log.value.level == "ERROR" end)
        warnings = Enum.filter(logs_list, fn log -> log.value.level == "WARN" end)
        
        %{
          application: app,
          errors: acc.errors ++ errors,
          warnings: acc.warnings ++ warnings,
          total_count: acc.total_count + length(logs_list),
          summary: %{
            error_count: length(acc.errors) + length(errors),
            warning_count: length(acc.warnings) + length(warnings)
          }
        }
      end
    )
    |> GenRocks.write_file(output_file, format: :json)
    |> Flow.run()
  end

  @doc """
  Sales data aggregation ETL.
  Reads multiple sales files, aggregates by region and product, writes summary.
  """
  def sales_aggregation_etl(sales_pattern, output_file) do
    Logger.info("Starting sales aggregation ETL: #{sales_pattern} -> #{output_file}")

    # Create sample sales data
    create_sample_sales_data(sales_pattern)

    GenRocks.from_source(GenRocks.Source.FileSource, %{
      glob_pattern: sales_pattern,
      format: :json,
      topic: "sales"
    })
    |> GenRocks.filter(fn msg ->
      # Filter valid sales records
      is_map(msg.value) && 
      Map.has_key?(msg.value, :amount) && 
      Map.has_key?(msg.value, :region) &&
      Map.has_key?(msg.value, :product)
    end)
    |> GenRocks.transform(fn msg ->
      # Normalize data
      normalized = %{
        region: String.upcase(String.trim(msg.value.region)),
        product: String.trim(msg.value.product),
        amount: if is_binary(msg.value.amount), 
                   do: String.to_float(msg.value.amount), 
                   else: msg.value.amount,
        date: msg.value.date,
        salesperson: msg.value.salesperson
      }
      %{msg | value: normalized}
    end)
    |> GenRocks.group_by_key(fn msg -> 
      {msg.value.region, msg.value.product}
    end)
    |> GenRocks.reduce(
      fn -> %{total_sales: 0.0, transaction_count: 0, salespeople: MapSet.new()} end,
      fn {{region, product}, sales}, acc ->
        sales_list = if is_list(sales), do: sales, else: [sales]
        
        total_amount = Enum.sum(Enum.map(sales_list, fn s -> s.value.amount end))
        
        salespeople = Enum.reduce(sales_list, acc.salespeople, fn sale, sp_set ->
          if sale.value.salesperson do
            MapSet.put(sp_set, sale.value.salesperson)
          else
            sp_set
          end
        end)

        %{
          region: region,
          product: product,
          total_sales: acc.total_sales + total_amount,
          transaction_count: acc.transaction_count + length(sales_list),
          unique_salespeople: MapSet.size(salespeople),
          avg_transaction_size: (acc.total_sales + total_amount) / (acc.transaction_count + length(sales_list))
        }
      end
    )
    |> GenRocks.transform(fn {key, summary} ->
      # Format final output
      %{
        region: summary.region,
        product: summary.product,
        metrics: %{
          total_sales: Float.round(summary.total_sales, 2),
          transaction_count: summary.transaction_count,
          avg_transaction_size: Float.round(summary.avg_transaction_size, 2),
          unique_salespeople: summary.unique_salespeople
        },
        generated_at: DateTime.utc_now()
      }
    end)
    |> GenRocks.write_file(output_file, format: :json)
    |> Flow.run()
  end

  @doc """
  Real-time data pipeline with file watching.
  Demonstrates continuous ETL processing.
  """
  def realtime_etl_pipeline(input_file, output_file) do
    Logger.info("Starting real-time ETL pipeline: #{input_file} -> #{output_file}")

    # Start file source producer that feeds into topic
    {:ok, _producer} = GenRocks.start_source_producer(
      GenRocks.Source.FileSource,
      %{
        file_path: input_file,
        format: :json,
        topic: "realtime_data",
        watch_mode: false  # File watching not implemented in POC
      },
      name: :realtime_producer
    )

    # Start topic for the pipeline
    {:ok, _} = GenRocks.start_topic("realtime_data", 4)

    # Start sink consumer that writes processed data
    {:ok, _consumer} = GenRocks.start_sink_consumer(
      GenRocks.Sink.FileSink,
      %{
        file_path: output_file,
        format: :json,
        rotation_size: 1024 * 1024,  # 1MB rotation
        compress: false
      },
      subscribe_to: [{:via, Registry, {GenRocks.ProducerRegistry, "realtime_data"}}],
      name: :realtime_sink
    )

    # Start processing consumer group
    processor_fn = fn message, _context ->
      processed_data = %{
        original: message.value,
        processed_at: DateTime.utc_now(),
        processing_time_ms: :rand.uniform(100),
        enriched: enrich_realtime_data(message.value)
      }

      # Publish processed data back to topic for sink consumer
      GenRocks.publish("realtime_data", processed_data, key: message.key)
      :ok
    end

    {:ok, _group} = GenRocks.start_consumer_group(
      "realtime_processors", 
      "realtime_data", 
      processor_fn,
      partitions: [0, 1, 2, 3],
      max_demand: 50
    )

    Logger.info("Real-time ETL pipeline started")
    :ok
  end

  @doc """
  Complex multi-stage ETL with validation and error handling.
  """
  def complex_etl_pipeline(input_pattern, valid_output, invalid_output, summary_output) do
    Logger.info("Starting complex ETL pipeline")

    # Create sample data
    create_complex_sample_data(input_pattern)

    # Stage 1: Read and validate
    {valid_flow, invalid_flow} = GenRocks.from_source(GenRocks.Source.FileSource, %{
      glob_pattern: input_pattern,
      format: :json,
      topic: "raw_data"
    })
    |> GenRocks.validate(fn msg ->
      case validate_record(msg.value) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
      end
    end)
    |> split_valid_invalid()

    # Stage 2: Process valid records
    valid_results = valid_flow
    |> GenRocks.transform(&normalize_valid_record/1)
    |> GenRocks.transform(&enrich_record/1)
    |> GenRocks.write_file(valid_output, format: :json)

    # Stage 3: Handle invalid records
    invalid_results = invalid_flow
    |> GenRocks.transform(fn {msg, reason} ->
      %{
        original_data: msg.value,
        validation_error: reason,
        timestamp: DateTime.utc_now(),
        requires_manual_review: true
      }
    end)
    |> GenRocks.write_file(invalid_output, format: :json)

    # Stage 4: Generate summary
    summary_results = valid_flow
    |> GenRocks.aggregate(:count)
    |> GenRocks.transform(fn count ->
      %{
        total_valid_records: count,
        processing_date: DateTime.utc_now(),
        pipeline: "complex_etl"
      }
    end)
    |> GenRocks.write_file(summary_output, format: :json)

    # Run all stages
    Task.async(fn -> Flow.run(valid_results) end)
    Task.async(fn -> Flow.run(invalid_results) end)
    Task.async(fn -> Flow.run(summary_results) end)
    |> Task.await_many(30_000)

    Logger.info("Complex ETL pipeline completed")
  end

  # Helper functions for data generation and processing

  defp create_sample_csv(file_path) do
    File.mkdir_p!(Path.dirname(file_path))
    
    csv_data = """
    id,name,email,age,department
    1,John Doe,john.doe@example.com,30,Engineering
    2,Jane Smith,jane.smith@example.com,25,Marketing
    3,Bob Johnson,bob.johnson@example.com,35,Sales
    4,Alice Brown,alice.brown@example.com,28,Engineering
    5,Charlie Wilson,charlie.wilson@example.com,32,HR
    """
    
    File.write!(file_path, csv_data)
    Logger.info("Created sample CSV: #{file_path}")
  end

  defp create_sample_logs(pattern) do
    # Create multiple log files based on pattern
    base_dir = Path.dirname(pattern)
    File.mkdir_p!(base_dir)

    log_files = ["app1.log", "app2.log", "app3.log"]
    
    Enum.each(log_files, fn filename ->
      file_path = Path.join(base_dir, filename)
      app_name = Path.basename(filename, ".log")
      
      log_content = generate_log_content(app_name)
      File.write!(file_path, log_content)
      Logger.info("Created sample log: #{file_path}")
    end)
  end

  defp generate_log_content(app_name) do
    levels = ["INFO", "WARN", "ERROR", "DEBUG"]
    
    1..50
    |> Enum.map(fn i ->
      level = Enum.random(levels)
      timestamp = DateTime.utc_now() |> DateTime.to_iso8601()
      message = "#{app_name} message #{i}"
      "#{timestamp} [#{level}] #{app_name}: #{message}"
    end)
    |> Enum.join("\n")
  end

  defp create_sample_sales_data(pattern) do
    base_dir = Path.dirname(pattern)
    File.mkdir_p!(base_dir)

    regions = ["NORTH", "SOUTH", "EAST", "WEST"]
    products = ["Widget A", "Widget B", "Widget C"]
    salespeople = ["Alice", "Bob", "Carol", "Dave"]

    Enum.each(regions, fn region ->
      file_path = Path.join(base_dir, "sales_#{String.downcase(region)}.json")
      
      sales_data = 1..20
      |> Enum.map(fn i ->
        %{
          region: region,
          product: Enum.random(products),
          amount: (:rand.uniform() * 1000) |> Float.round(2),
          date: Date.utc_today() |> Date.to_iso8601(),
          salesperson: Enum.random(salespeople)
        }
        |> Jason.encode!()
      end)
      |> Enum.join("\n")

      File.write!(file_path, sales_data)
      Logger.info("Created sample sales data: #{file_path}")
    end)
  end

  defp create_complex_sample_data(pattern) do
    base_dir = Path.dirname(pattern)
    File.mkdir_p!(base_dir)

    # Create mix of valid and invalid records
    records = Enum.map(1..100, fn i ->
      if rem(i, 7) == 0 do
        # Invalid record (missing required field)
        %{id: i, name: "Record #{i}"}
      else
        # Valid record
        %{
          id: i,
          name: "Record #{i}",
          email: "user#{i}@example.com",
          amount: :rand.uniform() * 1000,
          created_at: DateTime.utc_now() |> DateTime.to_iso8601()
        }
      end
    end)

    file_path = Path.join(base_dir, "complex_data.json")
    content = records
    |> Enum.map(&Jason.encode!/1)
    |> Enum.join("\n")

    File.write!(file_path, content)
    Logger.info("Created complex sample data: #{file_path}")
  end

  defp parse_log_line(line) do
    # Simple log parser: timestamp [level] app: message
    case Regex.run(~r/^(.+?) \[(.+?)\] (.+?): (.+)$/, String.trim(line)) do
      [_full, timestamp, level, app, message] ->
        {:ok, %{
          timestamp: timestamp,
          level: level,
          application: app,
          message: message,
          raw_line: line
        }}
      _ ->
        :error
    end
  end

  defp calculate_severity_score("ERROR"), do: 10
  defp calculate_severity_score("WARN"), do: 5
  defp calculate_severity_score("INFO"), do: 1
  defp calculate_severity_score(_), do: 0

  defp enrich_realtime_data(data) do
    # Simulate data enrichment
    %{
      category: classify_data(data),
      risk_score: calculate_risk_score(data),
      geo_location: determine_location()
    }
  end

  defp classify_data(_data) do
    Enum.random(["high_value", "medium_value", "low_value"])
  end

  defp calculate_risk_score(_data) do
    :rand.uniform() * 100
  end

  defp determine_location do
    Enum.random(["US-EAST", "US-WEST", "EU", "ASIA"])
  end

  defp split_valid_invalid(flow) do
    # This would need to be implemented as a proper Flow split
    # For now, returning the same flow for both (simplified)
    valid_flow = flow |> GenRocks.filter(fn 
      {:valid, _} -> true
      _ -> false
    end)
    |> GenRocks.transform(fn {:valid, msg} -> msg end)

    invalid_flow = flow |> GenRocks.filter(fn
      {:invalid, _, _} -> true
      _ -> false
    end)
    |> GenRocks.transform(fn {:invalid, msg, reason} -> {msg, reason} end)

    {valid_flow, invalid_flow}
  end

  defp validate_record(data) when is_map(data) do
    required_fields = [:id, :name, :email]
    
    missing_fields = Enum.filter(required_fields, fn field ->
      not Map.has_key?(data, field) or is_nil(Map.get(data, field))
    end)

    if length(missing_fields) == 0 do
      :ok
    else
      {:error, "Missing required fields: #{inspect(missing_fields)}"}
    end
  end

  defp validate_record(_), do: {:error, "Invalid data format"}

  defp normalize_valid_record({:valid, msg}) do
    normalized_data = %{
      id: msg.value.id,
      name: String.trim(msg.value.name),
      email: String.downcase(String.trim(msg.value.email)),
      amount: Map.get(msg.value, :amount, 0),
      normalized_at: DateTime.utc_now()
    }
    %{msg | value: normalized_data}
  end

  defp enrich_record(msg) do
    enriched_data = Map.merge(msg.value, %{
      enriched_at: DateTime.utc_now(),
      data_source: "etl_pipeline",
      quality_score: calculate_quality_score(msg.value)
    })
    %{msg | value: enriched_data}
  end

  defp calculate_quality_score(data) do
    # Simple quality scoring
    score = 0
    score = if Map.has_key?(data, :email) and String.contains?(data.email, "@"), do: score + 25, else: score
    score = if Map.has_key?(data, :name) and String.length(data.name) > 2, do: score + 25, else: score
    score = if Map.has_key?(data, :amount) and data.amount > 0, do: score + 25, else: score
    score + 25  # Base score
  end
end