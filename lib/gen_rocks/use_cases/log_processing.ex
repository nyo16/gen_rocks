defmodule GenRocks.UseCases.LogProcessing do
  @moduledoc """
  Distributed log processing use case.
  Handles log aggregation, parsing, alerting, and analytics similar to ELK stack.
  """

  require Logger
  alias GenRocks.Queue.{Supervisor, TopicProducer}

  @doc """
  Sets up distributed log processing pipeline.
  """
  def setup_pipeline do
    Logger.info("Setting up Log Processing Pipeline...")

    # Start topics for different log streams
    {:ok, _} = Supervisor.start_topic("raw_logs", 32)        # High throughput raw logs
    {:ok, _} = Supervisor.start_topic("parsed_logs", 16)     # Structured logs
    {:ok, _} = Supervisor.start_topic("error_logs", 8)       # Filtered error logs
    {:ok, _} = Supervisor.start_topic("alerts", 4)          # Critical alerts
    {:ok, _} = Supervisor.start_topic("metrics", 8)         # Extracted metrics

    # Start log processing consumers
    raw_log_processor = fn message, _context ->
      parse_and_route_log(message.value)
      :ok
    end

    {:ok, _} = Supervisor.start_consumer_group("log_parser", "raw_logs", raw_log_processor,
      partitions: 0..31 |> Enum.to_list(),
      max_demand: 200
    )

    # Start real-time processing flows
    spawn(fn -> run_error_detection() end)
    spawn(fn -> run_performance_monitoring() end)
    spawn(fn -> run_security_analysis() end)
    spawn(fn -> run_log_aggregation() end)

    Logger.info("Log Processing Pipeline setup completed")
    :ok
  end

  @doc """
  Simulates log generation from multiple applications.
  """
  def simulate_application_logs(apps \\ 5, logs_per_app \\ 1000) do
    Logger.info("Simulating logs from #{apps} applications, #{logs_per_app} logs each...")

    applications = 1..apps
    |> Enum.map(fn id ->
      %{
        name: "app_#{id}",
        version: "v1.#{:rand.uniform(5)}.#{:rand.uniform(10)}",
        environment: Enum.random(["prod", "staging", "dev"]),
        instance_id: "instance_#{:rand.uniform(100)}"
      }
    end)

    # Generate logs for each application
    tasks = Enum.map(applications, fn app ->
      Task.async(fn ->
        generate_app_logs(app, logs_per_app)
      end)
    end)

    Task.await_many(tasks, 60_000)
    Logger.info("Log simulation completed")
  end

  @doc """
  Runs error detection and alerting.
  """
  def run_error_detection do
    Logger.info("Starting error detection...")

    GenRocks.from_topic("parsed_logs", partition_count: 16)
    |> GenRocks.filter(fn msg -> 
      msg.value.level in ["ERROR", "FATAL", "CRITICAL"]
    end)
    |> GenRocks.window({:sliding, 60_000, 10_000})  # 1-minute sliding windows
    |> GenRocks.group_by_key(fn msg -> 
      {msg.value.application, msg.value.error_type}
    end)
    |> GenRocks.transform(&analyze_error_patterns/1)
    |> GenRocks.filter(fn {_key, analysis} -> 
      analysis.alert_triggered
    end)
    |> GenRocks.side_effect(fn {{app, error_type}, analysis} ->
      send_error_alert(app, error_type, analysis)
    end)
    |> GenRocks.run_to(fn _results -> :ok end)
  end

  @doc """
  Monitors application performance metrics from logs.
  """
  def run_performance_monitoring do
    Logger.info("Starting performance monitoring...")

    GenRocks.from_topic("parsed_logs", partition_count: 16)
    |> GenRocks.filter(fn msg ->
      Map.has_key?(msg.value, :response_time) || Map.has_key?(msg.value, :execution_time)
    end)
    |> GenRocks.window({:fixed, 120_000})  # 2-minute windows
    |> GenRocks.group_by_key(fn msg -> 
      {msg.value.application, msg.value.endpoint || msg.value.operation}
    end)
    |> GenRocks.reduce(
      fn -> %{
        count: 0,
        total_time: 0.0,
        min_time: nil,
        max_time: nil,
        p95_samples: []
      } end,
      fn {{app, endpoint}, logs}, acc ->
        logs_list = if is_list(logs), do: logs, else: [logs]
        times = Enum.map(logs_list, fn log -> 
          log.value.response_time || log.value.execution_time || 0
        end)
        
        new_min = if acc.min_time, do: min(acc.min_time, Enum.min(times)), else: Enum.min(times)
        new_max = if acc.max_time, do: max(acc.max_time, Enum.max(times)), else: Enum.max(times)
        
        %{
          count: acc.count + length(times),
          total_time: acc.total_time + Enum.sum(times),
          min_time: new_min,
          max_time: new_max,
          p95_samples: (acc.p95_samples ++ times) |> Enum.take(-1000),  # Keep last 1000 samples
          application: app,
          endpoint: endpoint
        }
      end
    )
    |> GenRocks.transform(&calculate_performance_metrics/1)
    |> GenRocks.side_effect(&publish_performance_metrics/1)
    |> GenRocks.run_to(fn _results -> :ok end)
  end

  @doc """
  Analyzes logs for security threats and suspicious patterns.
  """
  def run_security_analysis do
    Logger.info("Starting security analysis...")

    GenRocks.from_topic("parsed_logs", partition_count: 16)
    |> GenRocks.filter(&is_security_relevant/1)
    |> GenRocks.window({:sliding, 300_000, 60_000})  # 5-minute sliding windows, 1-minute slide
    |> GenRocks.group_by_key(fn msg -> 
      msg.value.source_ip || msg.value.user_id || "unknown"
    end)
    |> GenRocks.transform(&detect_security_threats/1)
    |> GenRocks.filter(fn {_source, threats} -> 
      length(threats) > 0
    end)
    |> GenRocks.side_effect(fn {source, threats} ->
      handle_security_threats(source, threats)
    end)
    |> GenRocks.run_to(fn _results -> :ok end)
  end

  @doc """
  Aggregates logs for analytics and reporting.
  """
  def run_log_aggregation do
    Logger.info("Starting log aggregation...")

    GenRocks.from_topic("parsed_logs", partition_count: 16)
    |> GenRocks.window({:fixed, 600_000})  # 10-minute aggregation windows
    |> GenRocks.group_by_key(fn msg -> 
      {msg.value.application, msg.value.level, msg.value.environment}
    end)
    |> GenRocks.reduce(
      fn -> %{
        count: 0,
        unique_users: MapSet.new(),
        unique_ips: MapSet.new(),
        error_codes: %{},
        timestamps: []
      } end,
      fn {{app, level, env}, logs}, acc ->
        logs_list = if is_list(logs), do: logs, else: [logs]
        
        users = Enum.reduce(logs_list, acc.unique_users, fn log, users ->
          if log.value.user_id, do: MapSet.put(users, log.value.user_id), else: users
        end)
        
        ips = Enum.reduce(logs_list, acc.unique_ips, fn log, ips ->
          if log.value.source_ip, do: MapSet.put(ips, log.value.source_ip), else: ips
        end)
        
        error_codes = Enum.reduce(logs_list, acc.error_codes, fn log, codes ->
          if log.value.status_code do
            Map.update(codes, log.value.status_code, 1, &(&1 + 1))
          else
            codes
          end
        end)
        
        %{
          count: acc.count + length(logs_list),
          unique_users: users,
          unique_ips: ips,
          error_codes: error_codes,
          timestamps: acc.timestamps ++ Enum.map(logs_list, fn l -> l.value.timestamp end),
          application: app,
          level: level,
          environment: env
        }
      end
    )
    |> GenRocks.transform(&create_analytics_summary/1)
    |> GenRocks.side_effect(&store_analytics/1)
    |> GenRocks.run_to(fn _results -> :ok end)
  end

  @doc """
  Creates custom log analysis pipeline with user-defined transformations.
  """
  def custom_log_analysis(filter_fn, transform_fn, aggregation_fn) do
    Logger.info("Running custom log analysis...")

    GenRocks.from_topic("parsed_logs", partition_count: 16)
    |> GenRocks.filter(filter_fn)
    |> GenRocks.transform(transform_fn)
    |> GenRocks.window({:fixed, 180_000})  # 3-minute windows
    |> GenRocks.group_by_key(fn msg -> msg.analysis_key end)
    |> GenRocks.reduce(
      fn -> %{items: [], custom_data: %{}} end,
      aggregation_fn
    )
    |> GenRocks.collect()
  end

  @doc """
  Creates log search functionality similar to Elasticsearch.
  """
  def search_logs(query_params, time_range \\ {DateTime.add(DateTime.utc_now(), -3600, :second), DateTime.utc_now()}) do
    Logger.info("Searching logs with query: #{inspect(query_params)}")
    
    {start_time, end_time} = time_range
    
    GenRocks.from_topic("parsed_logs", partition_count: 16)
    |> GenRocks.filter(fn msg ->
      in_time_range?(msg.value.timestamp, start_time, end_time) &&
      matches_query?(msg.value, query_params)
    end)
    |> GenRocks.transform(fn msg -> 
      # Add search relevance scoring
      score = calculate_relevance_score(msg.value, query_params)
      %{msg | search_score: score}
    end)
    |> GenRocks.collect()
    |> Enum.sort_by(& &1.search_score, :desc)
  end

  # Private helper functions

  defp generate_app_logs(app, count) do
    log_levels = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
    endpoints = ["/api/users", "/api/orders", "/api/products", "/login", "/checkout"]
    
    1..count
    |> Enum.each(fn i ->
      level = weighted_log_level()
      log_entry = %{
        timestamp: DateTime.utc_now(),
        level: level,
        application: app.name,
        version: app.version,
        environment: app.environment,
        instance_id: app.instance_id,
        message: generate_log_message(level, i),
        correlation_id: "req_#{:rand.uniform(10000)}",
        thread_id: "thread_#{:rand.uniform(20)}",
        endpoint: if(:rand.uniform() < 0.7, do: Enum.random(endpoints), else: nil),
        user_id: if(:rand.uniform() < 0.5, do: "user_#{:rand.uniform(1000)}", else: nil),
        source_ip: generate_ip_address(),
        user_agent: generate_user_agent(),
        response_time: if(:rand.uniform() < 0.6, do: :rand.uniform() * 1000, else: nil),
        status_code: if(:rand.uniform() < 0.6, do: weighted_status_code(), else: nil)
      }
      |> add_level_specific_fields(level)
      
      TopicProducer.publish("raw_logs", log_entry, key: app.name)
      
      # Add small delay to simulate realistic log streaming
      if rem(i, 100) == 0 do
        Process.sleep(1)
      end
    end)
  end

  defp parse_and_route_log(raw_log) do
    # Parse and structure the log entry
    parsed_log = raw_log
    |> add_parsed_fields()
    |> classify_log()
    |> extract_metrics()

    # Route to appropriate topics
    TopicProducer.publish("parsed_logs", parsed_log, key: parsed_log.application)

    # Route errors to error topic
    if parsed_log.level in ["ERROR", "FATAL", "CRITICAL"] do
      TopicProducer.publish("error_logs", parsed_log, key: "#{parsed_log.application}_error")
    end

    # Extract and route metrics
    if parsed_log.metrics do
      TopicProducer.publish("metrics", parsed_log.metrics, key: "#{parsed_log.application}_metrics")
    end
  end

  defp analyze_error_patterns({{app, error_type}, errors}) do
    errors_list = if is_list(errors), do: errors, else: [errors]
    
    error_count = length(errors_list)
    error_rate = error_count / 60  # errors per second (1-minute window)
    
    # Check for error rate thresholds
    alert_triggered = case error_type do
      "database_error" -> error_rate > 0.5
      "authentication_error" -> error_rate > 2.0
      "timeout_error" -> error_rate > 1.0
      _ -> error_rate > 3.0
    end

    recent_errors = Enum.take(errors_list, -5)
    
    analysis = %{
      error_count: error_count,
      error_rate: error_rate,
      alert_triggered: alert_triggered,
      severity: calculate_error_severity(error_type, error_rate),
      recent_examples: Enum.map(recent_errors, fn e -> e.value.message end),
      affected_users: errors_list 
        |> Enum.map(fn e -> e.value.user_id end)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()
        |> length()
    }

    {{app, error_type}, analysis}
  end

  defp calculate_performance_metrics({{app, endpoint}, stats}) do
    avg_time = if stats.count > 0, do: stats.total_time / stats.count, else: 0
    p95_time = calculate_percentile(stats.p95_samples, 95)
    
    metrics = %{
      application: stats.application,
      endpoint: stats.endpoint,
      window_start: DateTime.utc_now(),
      request_count: stats.count,
      avg_response_time: avg_time,
      min_response_time: stats.min_time,
      max_response_time: stats.max_time,
      p95_response_time: p95_time,
      throughput: stats.count / 120,  # requests per second (2-minute window)
      performance_grade: grade_performance(avg_time, p95_time)
    }

    {{app, endpoint}, metrics}
  end

  defp detect_security_threats({source, logs}) do
    logs_list = if is_list(logs), do: logs, else: [logs]
    
    threats = []
    threats = threats ++ detect_brute_force_attempts(logs_list)
    threats = threats ++ detect_sql_injection_attempts(logs_list)
    threats = threats ++ detect_suspicious_user_agents(logs_list)
    threats = threats ++ detect_rate_limiting_violations(logs_list)

    {source, threats}
  end

  defp send_error_alert(app, error_type, analysis) do
    alert = %{
      type: "error_rate_alert",
      application: app,
      error_type: error_type,
      severity: analysis.severity,
      error_count: analysis.error_count,
      error_rate: analysis.error_rate,
      affected_users: analysis.affected_users,
      examples: Enum.take(analysis.recent_examples, 3),
      timestamp: DateTime.utc_now()
    }

    Logger.warning("ERROR ALERT: #{app} - #{error_type} - Rate: #{analysis.error_rate}/sec")
    TopicProducer.publish("alerts", alert, key: "error_#{app}")
  end

  defp publish_performance_metrics({{app, endpoint}, metrics}) do
    Logger.info("Performance: #{app}#{endpoint} - Avg: #{Float.round(metrics.avg_response_time, 2)}ms, P95: #{Float.round(metrics.p95_response_time, 2)}ms")
    TopicProducer.publish("metrics", metrics, key: "perf_#{app}")
  end

  defp handle_security_threats(source, threats) do
    Enum.each(threats, fn threat ->
      Logger.warning("SECURITY THREAT: #{threat.type} from #{source} - Severity: #{threat.severity}")
      
      alert = %{
        type: "security_alert",
        threat_type: threat.type,
        source: source,
        severity: threat.severity,
        description: threat.description,
        evidence: threat.evidence,
        timestamp: DateTime.utc_now()
      }
      
      TopicProducer.publish("alerts", alert, key: "security_#{source}")
    end)
  end

  defp create_analytics_summary({{app, level, env}, stats}) do
    summary = %{
      application: stats.application,
      level: stats.level,
      environment: stats.environment,
      window_start: DateTime.utc_now(),
      log_count: stats.count,
      unique_users: MapSet.size(stats.unique_users),
      unique_ips: MapSet.size(stats.unique_ips),
      error_code_distribution: stats.error_codes,
      log_volume_trend: calculate_trend(stats.timestamps),
      top_error_codes: stats.error_codes 
        |> Enum.sort_by(&elem(&1, 1), :desc)
        |> Enum.take(5)
    }

    {{app, level, env}, summary}
  end

  defp store_analytics({{app, level, env}, summary}) do
    Logger.info("Analytics: #{app} #{env} #{level} - #{summary.log_count} logs, #{summary.unique_users} users")
    # In a real system, this would store to analytics database
    TopicProducer.publish("analytics_summary", summary, key: "analytics_#{app}")
  end

  # Helper functions for log generation and analysis

  defp weighted_log_level do
    case :rand.uniform() do
      x when x < 0.5 -> "INFO"
      x when x < 0.8 -> "DEBUG" 
      x when x < 0.93 -> "WARN"
      x when x < 0.98 -> "ERROR"
      _ -> "FATAL"
    end
  end

  defp weighted_status_code do
    case :rand.uniform() do
      x when x < 0.7 -> 200
      x when x < 0.8 -> 201
      x when x < 0.85 -> 301
      x when x < 0.9 -> 400
      x when x < 0.95 -> 404
      x when x < 0.98 -> 500
      _ -> 503
    end
  end

  defp generate_log_message(level, i) do
    case level do
      "DEBUG" -> "Processing request #{i} - step #{:rand.uniform(5)}"
      "INFO" -> "Request #{i} completed successfully in #{:rand.uniform(1000)}ms"
      "WARN" -> "Request #{i} took longer than expected: #{1000 + :rand.uniform(2000)}ms"
      "ERROR" -> Enum.random([
        "Database connection failed for request #{i}",
        "Authentication failed for request #{i}",
        "Timeout processing request #{i}",
        "Invalid input in request #{i}"
      ])
      "FATAL" -> "Critical system failure - request #{i} caused system instability"
    end
  end

  defp generate_ip_address do
    "#{:rand.uniform(255)}.#{:rand.uniform(255)}.#{:rand.uniform(255)}.#{:rand.uniform(255)}"
  end

  defp generate_user_agent do
    agents = [
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/14.1",
      "Mozilla/5.0 (X11; Linux x86_64) Firefox/89.0",
      "curl/7.68.0",
      "PostmanRuntime/7.28.0"
    ]
    Enum.random(agents)
  end

  defp add_level_specific_fields(log, level) do
    case level do
      "ERROR" -> 
        Map.merge(log, %{
          error_type: Enum.random(["database_error", "authentication_error", "timeout_error", "validation_error"]),
          stack_trace: "at com.example.Service.method(Service.java:#{:rand.uniform(500)})"
        })
      "FATAL" ->
        Map.merge(log, %{
          error_type: "system_failure",
          impact: "service_unavailable"
        })
      _ -> log
    end
  end

  defp add_parsed_fields(log) do
    # Add structured fields from parsing
    Map.merge(log, %{
      parsed_at: DateTime.utc_now(),
      log_id: "log_#{:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)}"
    })
  end

  defp classify_log(log) do
    # Classify log type and category
    category = cond do
      log.endpoint && String.contains?(log.endpoint, "api") -> "api_request"
      log.level in ["ERROR", "FATAL"] -> "error_event" 
      log.message && String.contains?(log.message, "auth") -> "authentication"
      true -> "general"
    end

    Map.put(log, :category, category)
  end

  defp extract_metrics(log) do
    # Extract metrics that can be used for monitoring
    metrics = if log.response_time do
      %{
        type: "performance_metric",
        application: log.application,
        endpoint: log.endpoint,
        response_time: log.response_time,
        timestamp: log.timestamp
      }
    else
      nil
    end

    Map.put(log, :metrics, metrics)
  end

  defp is_security_relevant(msg) do
    log = msg.value
    
    # Check for security-relevant patterns
    security_keywords = ["auth", "login", "password", "token", "permission", "access", "security"]
    suspicious_status_codes = [401, 403, 429]
    
    (log.level in ["WARN", "ERROR"]) ||
    (log.status_code in suspicious_status_codes) ||
    (log.message && Enum.any?(security_keywords, fn keyword -> 
      String.contains?(String.downcase(log.message), keyword)
    end))
  end

  defp detect_brute_force_attempts(logs) do
    auth_failures = Enum.filter(logs, fn log ->
      log.value.status_code == 401 || 
      (log.value.message && String.contains?(String.downcase(log.value.message), "auth"))
    end)

    if length(auth_failures) > 10 do
      [%{
        type: "brute_force_attack",
        severity: :high,
        description: "Multiple authentication failures detected",
        evidence: %{failure_count: length(auth_failures)}
      }]
    else
      []
    end
  end

  defp detect_sql_injection_attempts(logs) do
    sql_patterns = ["'", "union", "select", "drop", "delete", "--", "/*"]
    
    suspicious_logs = Enum.filter(logs, fn log ->
      log.value.message && 
      Enum.any?(sql_patterns, fn pattern ->
        String.contains?(String.downcase(log.value.message), pattern)
      end)
    end)

    if length(suspicious_logs) > 0 do
      [%{
        type: "sql_injection_attempt",
        severity: :critical,
        description: "Potential SQL injection patterns detected",
        evidence: %{suspicious_requests: length(suspicious_logs)}
      }]
    else
      []
    end
  end

  defp detect_suspicious_user_agents(logs) do
    suspicious_agents = ["sqlmap", "nikto", "dirb", "nmap"]
    
    malicious_requests = Enum.filter(logs, fn log ->
      log.value.user_agent &&
      Enum.any?(suspicious_agents, fn agent ->
        String.contains?(String.downcase(log.value.user_agent), agent)
      end)
    end)

    if length(malicious_requests) > 0 do
      [%{
        type: "malicious_user_agent",
        severity: :high,
        description: "Suspicious user agents detected",
        evidence: %{malicious_requests: length(malicious_requests)}
      }]
    else
      []
    end
  end

  defp detect_rate_limiting_violations(logs) do
    requests_per_minute = length(logs)
    
    if requests_per_minute > 1000 do  # Threshold for rate limiting
      [%{
        type: "rate_limit_violation",
        severity: :medium,
        description: "Excessive request rate detected",
        evidence: %{requests_per_minute: requests_per_minute}
      }]
    else
      []
    end
  end

  defp calculate_error_severity(error_type, error_rate) do
    base_severity = case error_type do
      "database_error" -> :high
      "authentication_error" -> :medium
      "timeout_error" -> :medium
      _ -> :low
    end

    # Increase severity based on rate
    cond do
      error_rate > 5.0 -> :critical
      error_rate > 2.0 && base_severity == :high -> :critical
      error_rate > 1.0 && base_severity != :low -> :high
      true -> base_severity
    end
  end

  defp calculate_percentile(samples, percentile) when length(samples) > 0 do
    sorted = Enum.sort(samples)
    index = trunc((percentile / 100) * length(sorted))
    Enum.at(sorted, min(index, length(sorted) - 1)) || 0
  end
  defp calculate_percentile(_, _), do: 0

  defp grade_performance(avg_time, p95_time) do
    cond do
      avg_time < 100 && p95_time < 200 -> "A"
      avg_time < 200 && p95_time < 500 -> "B"
      avg_time < 500 && p95_time < 1000 -> "C"
      avg_time < 1000 && p95_time < 2000 -> "D"
      true -> "F"
    end
  end

  defp calculate_trend(timestamps) when length(timestamps) > 1 do
    # Simple trend calculation - positive if increasing over time
    first_half = Enum.take(timestamps, div(length(timestamps), 2))
    second_half = Enum.drop(timestamps, div(length(timestamps), 2))
    
    if length(second_half) > length(first_half) do
      "increasing"
    else
      "stable"
    end
  end
  defp calculate_trend(_), do: "stable"

  defp in_time_range?(timestamp, start_time, end_time) do
    DateTime.compare(timestamp, start_time) != :lt &&
    DateTime.compare(timestamp, end_time) != :gt
  end

  defp matches_query?(log, query_params) do
    Enum.all?(query_params, fn {field, value} ->
      case Map.get(log, field) do
        nil -> false
        log_value -> 
          if is_binary(log_value) && is_binary(value) do
            String.contains?(String.downcase(log_value), String.downcase(value))
          else
            log_value == value
          end
      end
    end)
  end

  defp calculate_relevance_score(log, query_params) do
    # Simple relevance scoring based on field matches
    Enum.reduce(query_params, 0, fn {field, value}, score ->
      case Map.get(log, field) do
        nil -> score
        log_value when is_binary(log_value) and is_binary(value) ->
          if String.contains?(String.downcase(log_value), String.downcase(value)) do
            score + 1
          else
            score
          end
        log_value when log_value == value -> score + 2
        _ -> score
      end
    end)
  end
end