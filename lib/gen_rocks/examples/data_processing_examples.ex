defmodule GenRocks.Examples.DataProcessingExamples do
  @moduledoc """
  Examples demonstrating data processing pipelines using GenRocks with Flow.
  
  Shows how to combine GenRocks storage adapters with Flow for parallel data processing:
  - Real-time stream processing with ETS for hot data
  - Batch processing with DiskLog for durable pipelines  
  - ETL pipelines with mixed adapters
  - High-performance analytics with Flow parallel processing
  """
  
  require Logger
  alias GenRocks.Queue.FlowProcessor
  
  @doc """
  Real-time log analysis pipeline using ETS adapter for high-performance processing.
  
  Demonstrates:
  - High-throughput log ingestion
  - Parallel processing with Flow
  - Real-time analytics and alerting
  """
  def real_time_log_processing do
    Logger.info("=== Real-time Log Processing with ETS + Flow ===")
    
    # Start topic with ETS for maximum performance
    {:ok, _} = GenRocks.start_topic("application_logs", 4)
    
    # Simulate high-volume log ingestion
    spawn(fn ->
      log_types = ["INFO", "WARN", "ERROR", "DEBUG"]
      services = ["auth", "payments", "notifications", "analytics", "orders"]
      
      for i <- 1..1000 do
        log_entry = %{
          timestamp: System.system_time(:millisecond),
          level: Enum.random(log_types),
          service: Enum.random(services),
          message: "Operation #{i} completed",
          user_id: "user_#{rem(i, 100)}",
          request_id: "req_#{i}",
          duration_ms: :rand.uniform(500)
        }
        
        GenRocks.publish("application_logs", log_entry)
        
        if rem(i, 10) == 0 do
          Process.sleep(1)  # Brief pause to simulate realistic timing
        end
      end
      
      Logger.info("Log ingestion completed")
    end)
    
    # Set up Flow-based real-time processing pipeline
    {:ok, _consumer} = GenRocks.start_consumer_group("application_logs", "log_processor", fn message, _context ->
      # Process logs in parallel using Flow
      [message.value]
      |> Flow.from_enumerable()
      |> Flow.partition()
      |> Flow.map(fn log ->
        # Enrich log data
        Map.put(log, :processed_at, System.system_time(:millisecond))
      end)
      |> Flow.filter(fn log ->
        # Filter for important logs (WARN and ERROR)
        log.level in ["WARN", "ERROR"]
      end)
      |> Flow.map(fn log ->
        # Generate alerts for errors
        if log.level == "ERROR" do
          Logger.warn("ALERT: Error in #{log.service} - #{log.message}")
        end
        log
      end)
      |> Flow.run()
      
      :ok
    end)
    
    # Analytics aggregation pipeline
    spawn(fn ->
      Process.sleep(2000)  # Let some logs accumulate
      
      # Get recent logs for analysis
      logs_for_analysis = []  # In real system: query recent messages
      
      Logger.info("Running analytics on processed logs...")
      
      # Example: Service performance analysis
      logs_for_analysis
      |> Flow.from_enumerable()
      |> Flow.partition(key: fn log -> log.service end)
      |> Flow.group_by(fn log -> {log.service, log.level} end)
      |> Flow.map(fn {service_level, logs} ->
        {service, level} = service_level
        count = length(logs)
        avg_duration = if count > 0, do: Enum.sum(Enum.map(logs, & &1.duration_ms)) / count, else: 0
        %{service: service, level: level, count: count, avg_duration: avg_duration}
      end)
      |> Flow.run()
      |> Enum.each(fn stats ->
        Logger.info("Service #{stats.service} - #{stats.level}: #{stats.count} events, avg #{Float.round(stats.avg_duration, 1)}ms")
      end)
    end)
    
    Process.sleep(5000)
    
    GenRocks.stop_topic("application_logs")
    Logger.info("Real-time log processing completed")
    
    {:ok, :completed}
  end
  
  @doc """
  ETL (Extract, Transform, Load) pipeline using DiskLog for data durability.
  
  Demonstrates:
  - Reliable data extraction with recovery capabilities
  - Complex transformations using Flow
  - Persistent intermediate results
  """
  def etl_pipeline_with_disk_log do
    Logger.info("=== ETL Pipeline with DiskLog + Flow ===")
    
    # Set up durable topics for each stage
    {:ok, _} = GenRocks.start_topic_with_disk_log("raw_data", 2,
      log_dir: "./data_processing_examples/raw",
      max_no_bytes: 50 * 1024 * 1024
    )
    
    {:ok, _} = GenRocks.start_topic_with_disk_log("transformed_data", 2,
      log_dir: "./data_processing_examples/transformed", 
      max_no_bytes: 50 * 1024 * 1024
    )
    
    {:ok, _} = GenRocks.start_topic_with_disk_log("processed_data", 1,
      log_dir: "./data_processing_examples/processed",
      max_no_bytes: 50 * 1024 * 1024
    )
    
    Logger.info("Started durable ETL pipeline topics")
    
    # Step 1: Extract - Simulate data extraction from various sources
    raw_data = [
      # E-commerce transactions
      %{source: "orders", type: "purchase", user_id: "u1", amount: 99.99, product: "laptop", timestamp: System.system_time(:millisecond)},
      %{source: "orders", type: "purchase", user_id: "u2", amount: 29.99, product: "book", timestamp: System.system_time(:millisecond)},
      %{source: "orders", type: "refund", user_id: "u1", amount: -99.99, product: "laptop", timestamp: System.system_time(:millisecond)},
      
      # User activity
      %{source: "activity", type: "login", user_id: "u1", ip: "192.168.1.100", timestamp: System.system_time(:millisecond)},
      %{source: "activity", type: "page_view", user_id: "u1", page: "/products", timestamp: System.system_time(:millisecond)},
      %{source: "activity", type: "logout", user_id: "u1", session_duration: 1800, timestamp: System.system_time(:millisecond)},
      
      # System metrics
      %{source: "metrics", type: "cpu_usage", server: "web1", value: 75.5, timestamp: System.system_time(:millisecond)},
      %{source: "metrics", type: "memory_usage", server: "web1", value: 60.2, timestamp: System.system_time(:millisecond)},
      %{source: "metrics", type: "disk_usage", server: "db1", value: 45.8, timestamp: System.system_time(:millisecond)}
    ]
    
    Logger.info("Extracting #{length(raw_data)} records...")
    
    Enum.each(raw_data, fn record ->
      GenRocks.publish("raw_data", record)
    end)
    
    # Step 2: Transform - Process raw data with Flow
    {:ok, _transformer} = GenRocks.start_consumer_group("raw_data", "transformer", fn message, _context ->
      [message.value]
      |> Flow.from_enumerable()
      |> Flow.map(fn record ->
        # Data cleansing and enrichment
        record
        |> Map.put(:processed_timestamp, System.system_time(:millisecond))
        |> Map.put(:batch_id, "batch_#{div(System.system_time(:millisecond), 1000)}")
        |> transform_by_source()
      end)
      |> Flow.filter(fn record ->
        # Data validation
        Map.get(record, :valid, true)
      end)
      |> Flow.run()
      |> Enum.each(fn transformed_record ->
        GenRocks.publish("transformed_data", transformed_record)
      end)
      
      :ok
    end)
    
    Process.sleep(2000)  # Allow transformation to complete
    
    # Step 3: Load - Aggregate and finalize data
    {:ok, _loader} = GenRocks.start_consumer_group("transformed_data", "loader", fn message, _context ->
      # Batch processing for efficiency
      :timer.sleep(100)  # Simulate batch accumulation
      
      [message.value]
      |> Flow.from_enumerable()
      |> Flow.partition(key: fn record -> record.source end)
      |> Flow.group_by(fn record -> {record.source, record.type} end)
      |> Flow.map(fn {source_type, records} ->
        {source, type} = source_type
        
        # Aggregate by source and type
        summary = %{
          source: source,
          type: type,
          count: length(records),
          batch_timestamp: System.system_time(:millisecond),
          sample_record: List.first(records)
        }
        
        # Add source-specific aggregations
        case source do
          "orders" ->
            total_amount = records |> Enum.map(& &1.amount) |> Enum.sum()
            Map.put(summary, :total_amount, total_amount)
            
          "metrics" ->
            avg_value = records |> Enum.map(& &1.value) |> Enum.sum() |> Kernel./(length(records))
            Map.put(summary, :average_value, avg_value)
            
          _ ->
            summary
        end
      end)
      |> Flow.run()
      |> Enum.each(fn summary ->
        GenRocks.publish("processed_data", summary)
        Logger.info("Loaded summary: #{summary.source}/#{summary.type} - #{summary.count} records")
      end)
      
      :ok
    end)
    
    # Monitor final results
    {:ok, _monitor} = GenRocks.start_consumer_group("processed_data", "monitor", fn message, _context ->
      Logger.info("ETL Result: #{inspect(message.value)}")
      :ok
    end)
    
    Process.sleep(5000)  # Allow full pipeline to complete
    
    Logger.info("ETL pipeline completed - all data persisted to disk logs")
    
    GenRocks.stop_topic("raw_data")
    GenRocks.stop_topic("transformed_data") 
    GenRocks.stop_topic("processed_data")
    
    {:ok, :completed}
  end
  
  @doc """
  High-performance analytics using hybrid storage approach.
  
  Uses ETS for hot/recent data and DiskLog for historical data and results.
  """
  def hybrid_analytics_pipeline do
    Logger.info("=== Hybrid Analytics Pipeline ===")
    
    # Hot data (last 1 hour) - ETS for speed
    {:ok, _} = GenRocks.start_topic("hot_events", 8)  # ETS, high partition count
    
    # Historical data - DiskLog for persistence  
    {:ok, _} = GenRocks.start_topic_with_disk_log("historical_events", 4,
      log_dir: "./data_processing_examples/historical",
      max_no_files: 50,
      max_no_bytes: 100 * 1024 * 1024
    )
    
    # Analytics results - DiskLog for permanent storage
    {:ok, _} = GenRocks.start_topic_with_disk_log("analytics_results", 2,
      log_dir: "./data_processing_examples/results",
      max_no_files: 20,
      max_no_bytes: 50 * 1024 * 1024
    )
    
    Logger.info("Started hybrid analytics topics")
    
    # Generate mixed time-series data
    spawn(fn ->
      Logger.info("Generating time-series data...")
      
      for hour <- 0..23 do  # 24 hours of data
        for minute <- 0..59, rem(minute, 5) == 0 do  # Every 5 minutes
          timestamp = System.system_time(:millisecond) - ((23 - hour) * 3600 + minute * 60) * 1000
          
          # Generate various metrics
          metrics = [
            %{metric: "page_views", value: :rand.uniform(1000) + hour * 10, timestamp: timestamp},
            %{metric: "user_signups", value: :rand.uniform(50) + hour, timestamp: timestamp},
            %{metric: "revenue", value: Float.round(:rand.uniform() * 10000 + hour * 100, 2), timestamp: timestamp},
            %{metric: "api_calls", value: :rand.uniform(5000) + hour * 50, timestamp: timestamp}
          ]
          
          Enum.each(metrics, fn metric ->
            current_time = System.system_time(:millisecond)
            
            if timestamp > (current_time - 3600 * 1000) do
              # Recent data goes to hot storage (ETS)
              GenRocks.publish("hot_events", metric)
            else
              # Historical data goes to persistent storage (DiskLog) 
              GenRocks.publish("historical_events", metric)
            end
          end)
        end
      end
      
      Logger.info("Data generation completed")
    end)
    
    # Real-time analytics on hot data
    {:ok, _hot_analyzer} = GenRocks.start_consumer_group("hot_events", "hot_analyzer", fn message, _context ->
      [message.value]
      |> Flow.from_enumerable()
      |> Flow.partition(key: fn event -> event.metric end)
      |> Flow.group_by(fn event -> event.metric end)
      |> Flow.reduce(fn -> %{} end, fn event, acc ->
        metric = event.metric
        current = Map.get(acc, metric, %{count: 0, sum: 0, latest: 0})
        
        %{acc | metric => %{
          count: current.count + 1,
          sum: current.sum + event.value,
          latest: max(current.latest, event.timestamp)
        }}
      end)
      |> Flow.map(fn {metric, stats} ->
        %{
          type: "realtime_stats",
          metric: metric,
          count: stats.count,
          average: if(stats.count > 0, do: stats.sum / stats.count, else: 0),
          latest_timestamp: stats.latest,
          window: "realtime",
          calculated_at: System.system_time(:millisecond)
        }
      end)
      |> Flow.run()
      |> Enum.each(fn result ->
        GenRocks.publish("analytics_results", result)
      end)
      
      :ok
    end)
    
    # Batch analytics on historical data  
    spawn(fn ->
      Process.sleep(3000)  # Let some data accumulate
      
      Logger.info("Running batch analytics on historical data...")
      
      # Simulate reading historical data for analysis
      # In real implementation, this would scan the historical_events topic
      
      historical_sample = [
        %{metric: "page_views", value: 850, timestamp: System.system_time(:millisecond) - 7200000},
        %{metric: "page_views", value: 920, timestamp: System.system_time(:millisecond) - 3600000},
        %{metric: "revenue", value: 5500.0, timestamp: System.system_time(:millisecond) - 7200000},
        %{metric: "revenue", value: 6200.0, timestamp: System.system_time(:millisecond) - 3600000}
      ]
      
      # Complex batch analysis with Flow
      historical_sample
      |> Flow.from_enumerable()
      |> Flow.partition(key: fn event -> event.metric end)
      |> Flow.group_by(fn event -> 
        # Group by metric and hour
        hour = div(event.timestamp, 3600000) * 3600000
        {event.metric, hour}
      end)
      |> Flow.map(fn {metric_hour, events} ->
        {metric, hour} = metric_hour
        
        %{
          type: "historical_analysis",
          metric: metric,
          hour_bucket: hour,
          count: length(events),
          min: events |> Enum.map(& &1.value) |> Enum.min(),
          max: events |> Enum.map(& &1.value) |> Enum.max(),
          avg: events |> Enum.map(& &1.value) |> Enum.sum() |> Kernel./(length(events)),
          calculated_at: System.system_time(:millisecond)
        }
      end)
      |> Flow.run()
      |> Enum.each(fn analysis ->
        GenRocks.publish("analytics_results", analysis)
        Logger.info("Historical analysis: #{analysis.metric} - avg: #{Float.round(analysis.avg, 1)}")
      end)
    end)
    
    # Results monitor
    {:ok, _monitor} = GenRocks.start_consumer_group("analytics_results", "results_monitor", fn message, _context ->
      result = message.value
      Logger.info("Analytics Result [#{result.type}]: #{result.metric} - #{inspect(Map.drop(result, [:type, :metric]))}")
      :ok
    end)
    
    Process.sleep(8000)
    
    Logger.info("Hybrid analytics pipeline completed")
    Logger.info("Hot data processed in ETS for real-time insights")
    Logger.info("Historical data analyzed from DiskLog for batch insights")
    Logger.info("Results persisted to DiskLog for permanent storage")
    
    GenRocks.stop_topic("hot_events")
    GenRocks.stop_topic("historical_events") 
    GenRocks.stop_topic("analytics_results")
    
    {:ok, :completed}
  end
  
  @doc """
  Machine learning data pipeline with feature engineering.
  """
  def ml_feature_pipeline do
    Logger.info("=== ML Feature Engineering Pipeline ===")
    
    # Raw training data - persistent for reproducibility
    {:ok, _} = GenRocks.start_topic_with_disk_log("raw_training_data", 4,
      log_dir: "./data_processing_examples/ml/raw"
    )
    
    # Feature store - ETS for fast access during training
    {:ok, _} = GenRocks.start_topic("feature_store", 8)
    
    # Model training data - persistent for experiments
    {:ok, _} = GenRocks.start_topic_with_disk_log("training_features", 2,
      log_dir: "./data_processing_examples/ml/features"
    )
    
    Logger.info("Started ML pipeline topics")
    
    # Generate sample training data (user behavior + outcomes)
    training_samples = for i <- 1..500 do
      user_id = "user_#{rem(i, 100)}"
      
      %{
        user_id: user_id,
        age: 18 + :rand.uniform(60),
        session_duration: :rand.uniform(3600),
        pages_viewed: :rand.uniform(20),
        previous_purchases: :rand.uniform(10),
        device_type: Enum.random(["mobile", "desktop", "tablet"]),
        referrer: Enum.random(["google", "facebook", "direct", "email"]),
        time_of_day: :rand.uniform(24),
        day_of_week: :rand.uniform(7),
        # Target variable (will user purchase?)
        purchased: :rand.uniform() > 0.7,
        purchase_amount: if(:rand.uniform() > 0.7, do: Float.round(:rand.uniform() * 500, 2), else: 0),
        timestamp: System.system_time(:millisecond)
      }
    end
    
    Logger.info("Publishing #{length(training_samples)} training samples...")
    
    Enum.each(training_samples, fn sample ->
      GenRocks.publish("raw_training_data", sample)
    end)
    
    # Feature engineering pipeline
    {:ok, _feature_engineer} = GenRocks.start_consumer_group("raw_training_data", "feature_engineer", fn message, _context ->
      [message.value]
      |> Flow.from_enumerable()
      |> Flow.map(fn sample ->
        # Feature engineering with Flow
        engineered_features = sample
        |> add_derived_features()
        |> add_behavioral_features()
        |> add_temporal_features()
        |> normalize_features()
        
        engineered_features
      end)
      |> Flow.run()
      |> Enum.each(fn features ->
        # Store in fast feature store for ML training access
        GenRocks.publish("feature_store", features)
        
        # Also persist for experiment reproducibility  
        GenRocks.publish("training_features", features)
      end)
      
      :ok
    end)
    
    # Simulate ML model training consumer
    {:ok, _trainer} = GenRocks.start_consumer_group("feature_store", "model_trainer", fn message, _context ->
      features = message.value
      
      # Simulate batch collection for training
      if rem(features.user_id |> String.replace("user_", "") |> String.to_integer(), 50) == 0 do
        Logger.info("Training batch ready - features for user #{features.user_id}")
        Logger.info("  Features: #{length(Map.keys(features))} dimensions")
        Logger.info("  Target: purchased=#{features.purchased}, amount=$#{features.purchase_amount}")
      end
      
      :ok
    end)
    
    Process.sleep(5000)
    
    Logger.info("ML feature pipeline completed")
    Logger.info("Raw data preserved in DiskLog for reproducible experiments")
    Logger.info("Features available in ETS for fast ML training access")
    Logger.info("Engineered features persisted for model versioning")
    
    GenRocks.stop_topic("raw_training_data")
    GenRocks.stop_topic("feature_store")
    GenRocks.stop_topic("training_features")
    
    {:ok, :completed}
  end
  
  # Helper functions for feature engineering
  
  defp add_derived_features(sample) do
    sample
    |> Map.put(:pages_per_minute, sample.pages_viewed / max(sample.session_duration / 60, 1))
    |> Map.put(:engagement_score, sample.session_duration * sample.pages_viewed / 100)
    |> Map.put(:is_returning_user, sample.previous_purchases > 0)
  end
  
  defp add_behavioral_features(sample) do
    # Device type encoding
    device_features = case sample.device_type do
      "mobile" -> %{is_mobile: 1, is_desktop: 0, is_tablet: 0}
      "desktop" -> %{is_mobile: 0, is_desktop: 1, is_tablet: 0}  
      "tablet" -> %{is_mobile: 0, is_desktop: 0, is_tablet: 1}
    end
    
    # Referrer features
    referrer_features = case sample.referrer do
      "google" -> %{from_search: 1, from_social: 0, from_direct: 0, from_email: 0}
      "facebook" -> %{from_search: 0, from_social: 1, from_direct: 0, from_email: 0}
      "direct" -> %{from_search: 0, from_social: 0, from_direct: 1, from_email: 0}
      "email" -> %{from_search: 0, from_social: 0, from_direct: 0, from_email: 1}
    end
    
    Map.merge(sample, Map.merge(device_features, referrer_features))
  end
  
  defp add_temporal_features(sample) do
    sample
    |> Map.put(:is_weekend, sample.day_of_week in [6, 7])
    |> Map.put(:is_business_hours, sample.time_of_day >= 9 && sample.time_of_day <= 17)
    |> Map.put(:is_evening, sample.time_of_day >= 18 && sample.time_of_day <= 22)
  end
  
  defp normalize_features(sample) do
    sample
    |> Map.put(:age_normalized, (sample.age - 40) / 20)  # Center around 40, scale by 20
    |> Map.put(:session_duration_log, :math.log(sample.session_duration + 1))
    |> Map.put(:pages_viewed_sqrt, :math.sqrt(sample.pages_viewed))
  end
  
  defp transform_by_source(record) do
    case record.source do
      "orders" ->
        record
        |> Map.put(:amount_category, categorize_amount(record.amount))
        |> Map.put(:valid, record.amount != 0)
        
      "activity" ->
        record
        |> Map.put(:session_type, categorize_session(record))
        |> Map.put(:valid, true)
        
      "metrics" ->
        record
        |> Map.put(:alert_level, categorize_metric(record))
        |> Map.put(:valid, record.value >= 0)
        
      _ ->
        Map.put(record, :valid, true)
    end
  end
  
  defp categorize_amount(amount) when amount >= 100, do: "high"
  defp categorize_amount(amount) when amount >= 50, do: "medium" 
  defp categorize_amount(amount) when amount > 0, do: "low"
  defp categorize_amount(_), do: "refund"
  
  defp categorize_session(%{type: "login"}), do: "start"
  defp categorize_session(%{type: "logout"}), do: "end"
  defp categorize_session(_), do: "activity"
  
  defp categorize_metric(%{type: type, value: value}) do
    threshold = case type do
      "cpu_usage" -> 80
      "memory_usage" -> 85
      "disk_usage" -> 90
      _ -> 100
    end
    
    if value >= threshold, do: "critical", else: "normal"
  end
  
  @doc """
  Runs all data processing examples.
  """
  def run_all_examples do
    Logger.info("Running all data processing examples...")
    
    examples = [
      {:real_time_log_processing, "Real-time Log Processing"},
      {:etl_pipeline_with_disk_log, "ETL Pipeline with DiskLog"},
      {:hybrid_analytics_pipeline, "Hybrid Analytics Pipeline"},
      {:ml_feature_pipeline, "ML Feature Engineering Pipeline"}
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
    Logger.info("All data processing examples completed!")
    Logger.info("Check ./data_processing_examples/ for generated files")
    Logger.info(String.duplicate("=", 60))
    
    {:ok, results}
  end
  
  @doc """
  Cleans up example data directories.
  """
  def cleanup_examples do
    dirs_to_clean = [
      "./data_processing_examples"
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