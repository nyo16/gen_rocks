defmodule GenRocks.UseCases.IoTProcessing do
  @moduledoc """
  IoT sensor data processing use case.
  Handles real-time sensor data ingestion, anomaly detection, and alerting.
  """

  require Logger
  alias GenRocks.Queue.{Supervisor, TopicProducer}

  @doc """
  Sets up IoT data processing pipeline.
  """
  def setup_pipeline do
    Logger.info("Setting up IoT Processing Pipeline...")

    # Start topics for different data streams
    {:ok, _} = Supervisor.start_topic("sensor_readings", 16)  # High throughput
    {:ok, _} = Supervisor.start_topic("anomaly_alerts", 4)
    {:ok, _} = Supervisor.start_topic("device_health", 4)
    {:ok, _} = Supervisor.start_topic("aggregated_metrics", 8)

    # Start data ingestion processors
    ingestion_consumer = fn message, _context ->
      process_sensor_reading(message.value)
      :ok
    end

    {:ok, _} = Supervisor.start_consumer_group("sensor_processor", "sensor_readings", ingestion_consumer,
      partitions: 0..15 |> Enum.to_list(),
      max_demand: 100
    )

    # Start real-time processing flows
    spawn(fn -> run_anomaly_detection() end)
    spawn(fn -> run_device_health_monitoring() end)
    spawn(fn -> run_data_aggregation() end)

    Logger.info("IoT Processing Pipeline setup completed")
    :ok
  end

  @doc """
  Simulates IoT sensor data generation.
  """
  def simulate_sensor_data(device_count \\ 100, duration_minutes \\ 5) do
    Logger.info("Simulating sensor data for #{device_count} devices for #{duration_minutes} minutes...")

    end_time = DateTime.add(DateTime.utc_now(), duration_minutes * 60, :second)
    
    # Create device definitions
    devices = 1..device_count
    |> Enum.map(fn id ->
      %{
        device_id: "device_#{String.pad_leading("#{id}", 4, "0")}",
        type: Enum.random(["temperature", "humidity", "pressure", "motion", "light"]),
        location: generate_location(),
        normal_range: generate_normal_range()
      }
    end)

    # Start data generation tasks
    tasks = Enum.map(devices, fn device ->
      Task.async(fn -> 
        generate_device_readings(device, end_time)
      end)
    end)

    # Wait for all tasks to complete
    Task.await_many(tasks, :infinity)
    Logger.info("Sensor data simulation completed")
  end

  @doc """
  Runs anomaly detection on sensor readings.
  """
  def run_anomaly_detection do
    Logger.info("Starting anomaly detection...")

    GenRocks.from_topic("sensor_readings", partition_count: 16)
    |> GenRocks.window({:sliding, 60_000, 10_000})  # 1-minute sliding windows
    |> GenRocks.group_by_key(fn msg -> msg.value.device_id end)
    |> GenRocks.transform(&detect_anomalies/1)
    |> GenRocks.filter(fn {_device_id, anomalies} -> length(anomalies) > 0 end)
    |> GenRocks.side_effect(fn {device_id, anomalies} ->
      Enum.each(anomalies, fn anomaly ->
        Logger.warning("ANOMALY DETECTED: Device #{device_id} - #{anomaly.type}: #{anomaly.value}")
        TopicProducer.publish("anomaly_alerts", %{
          device_id: device_id,
          anomaly_type: anomaly.type,
          value: anomaly.value,
          severity: anomaly.severity,
          timestamp: DateTime.utc_now()
        }, key: device_id)
      end)
    end)
    |> GenRocks.run_to(fn _results -> :ok end)
  end

  @doc """
  Monitors device health and connectivity.
  """
  def run_device_health_monitoring do
    Logger.info("Starting device health monitoring...")

    GenRocks.from_topic("sensor_readings", partition_count: 16)
    |> GenRocks.window({:fixed, 120_000})  # 2-minute fixed windows
    |> GenRocks.group_by_key(fn msg -> msg.value.device_id end)
    |> GenRocks.transform(&analyze_device_health/1)
    |> GenRocks.filter(fn {_device_id, health} -> health.status != :healthy end)
    |> GenRocks.side_effect(fn {device_id, health} ->
      Logger.info("Device Health Update: #{device_id} - Status: #{health.status}")
      TopicProducer.publish("device_health", %{
        device_id: device_id,
        status: health.status,
        last_seen: health.last_reading,
        reading_count: health.reading_count,
        battery_level: health.battery_level,
        timestamp: DateTime.utc_now()
      }, key: device_id)
    end)
    |> GenRocks.run_to(fn _results -> :ok end)
  end

  @doc """
  Aggregates sensor data for analytics and reporting.
  """
  def run_data_aggregation do
    Logger.info("Starting data aggregation...")

    GenRocks.from_topic("sensor_readings", partition_count: 16)
    |> GenRocks.window({:fixed, 300_000})  # 5-minute aggregation windows
    |> GenRocks.group_by_key(fn msg -> 
      {msg.value.device_type, msg.value.location.zone}
    end)
    |> GenRocks.reduce(
      fn -> %{
        count: 0,
        sum: 0.0,
        min: nil,
        max: nil,
        readings: []
      } end,
      fn {{device_type, zone}, readings}, acc ->
        readings_list = if is_list(readings), do: readings, else: [readings]
        values = Enum.map(readings_list, fn r -> r.value.reading end)
        
        new_min = if acc.min, do: min(acc.min, Enum.min(values)), else: Enum.min(values)
        new_max = if acc.max, do: max(acc.max, Enum.max(values)), else: Enum.max(values)
        
        %{
          count: acc.count + length(values),
          sum: acc.sum + Enum.sum(values),
          min: new_min,
          max: new_max,
          device_type: device_type,
          zone: zone
        }
      end
    )
    |> GenRocks.transform(fn {key, stats} ->
      average = if stats.count > 0, do: stats.sum / stats.count, else: 0.0
      
      %{
        device_type: stats.device_type,
        zone: stats.zone,
        window_start: DateTime.utc_now(),
        reading_count: stats.count,
        average: average,
        min: stats.min,
        max: stats.max,
        variance: calculate_variance(stats)
      }
    end)
    |> GenRocks.side_effect(fn aggregated_data ->
      TopicProducer.publish("aggregated_metrics", aggregated_data,
        key: "#{aggregated_data.device_type}_#{aggregated_data.zone}"
      )
    end)
    |> GenRocks.run_to(fn _results -> :ok end)
  end

  @doc """
  Predictive maintenance analysis.
  """
  def predictive_maintenance_analysis do
    Logger.info("Running predictive maintenance analysis...")

    GenRocks.from_topic("sensor_readings", partition_count: 16)
    |> GenRocks.filter(fn msg -> 
      msg.value.device_type in ["temperature", "pressure", "vibration"]
    end)
    |> GenRocks.window({:fixed, 600_000})  # 10-minute windows
    |> GenRocks.group_by_key(fn msg -> msg.value.device_id end)
    |> GenRocks.transform(&predict_maintenance_needs/1)
    |> GenRocks.filter(fn {_device_id, prediction} -> 
      prediction.maintenance_score > 0.7
    end)
    |> GenRocks.side_effect(fn {device_id, prediction} ->
      Logger.warning("MAINTENANCE REQUIRED: Device #{device_id} - Score: #{prediction.maintenance_score}")
      schedule_maintenance(device_id, prediction)
    end)
    |> GenRocks.collect()
  end

  @doc """
  Environmental monitoring dashboard data.
  """
  def environmental_dashboard_feed do
    Logger.info("Starting environmental dashboard feed...")

    GenRocks.from_topic("sensor_readings", partition_count: 16)
    |> GenRocks.filter(fn msg ->
      msg.value.device_type in ["temperature", "humidity", "air_quality"]
    end)
    |> GenRocks.window({:fixed, 30_000})  # 30-second updates
    |> GenRocks.group_by_key(fn msg -> 
      msg.value.location.zone
    end)
    |> GenRocks.transform(&create_dashboard_update/1)
    |> GenRocks.side_effect(&publish_to_dashboard/1)
    |> GenRocks.run_to(fn _results -> :ok end)
  end

  # Private helper functions

  defp process_sensor_reading(reading) do
    # Validate and enrich sensor reading
    processed_reading = reading
    |> validate_reading()
    |> add_derived_metrics()
    |> add_geospatial_context()

    # Store in time-series database (simulated)
    store_reading(processed_reading)
  end

  defp generate_device_readings(device, end_time) do
    Stream.iterate(DateTime.utc_now(), fn time ->
      DateTime.add(time, Enum.random(5..15), :second)
    end)
    |> Stream.take_while(fn time -> DateTime.before?(time, end_time) end)
    |> Enum.each(fn timestamp ->
      reading = generate_sensor_reading(device, timestamp)
      TopicProducer.publish("sensor_readings", reading, key: device.device_id)
      Process.sleep(Enum.random(10..100))  # Simulate realistic intervals
    end)
  end

  defp generate_sensor_reading(device, timestamp) do
    # Generate realistic sensor reading with occasional anomalies
    base_value = case device.type do
      "temperature" -> 20.0 + :rand.normal() * 5
      "humidity" -> 50.0 + :rand.normal() * 15
      "pressure" -> 1013.25 + :rand.normal() * 10
      "motion" -> if :rand.uniform() < 0.1, do: 1, else: 0
      "light" -> :rand.uniform() * 1000
    end

    # Introduce anomalies 2% of the time
    reading_value = if :rand.uniform() < 0.02 do
      # Generate anomalous reading
      base_value * (1 + (:rand.uniform() - 0.5) * 4)
    else
      base_value
    end

    %{
      device_id: device.device_id,
      device_type: device.type,
      location: device.location,
      reading: reading_value,
      unit: get_unit_for_type(device.type),
      timestamp: timestamp,
      battery_level: max(0, 100 - :rand.uniform() * 0.1),  # Slow battery drain
      signal_strength: -30 - :rand.uniform() * 50
    }
  end

  defp detect_anomalies({device_id, readings}) do
    readings_list = if is_list(readings), do: readings, else: [readings]
    
    anomalies = readings_list
    |> Enum.filter(fn reading ->
      is_anomalous_reading(reading.value)
    end)
    |> Enum.map(fn reading ->
      %{
        type: determine_anomaly_type(reading.value),
        value: reading.value.reading,
        severity: calculate_anomaly_severity(reading.value),
        timestamp: reading.value.timestamp
      }
    end)

    {device_id, anomalies}
  end

  defp analyze_device_health({device_id, readings}) do
    readings_list = if is_list(readings), do: readings, else: [readings]
    
    if length(readings_list) == 0 do
      {device_id, %{status: :offline, last_reading: nil, reading_count: 0, battery_level: 0}}
    else
      latest_reading = Enum.max_by(readings_list, fn r -> r.value.timestamp end)
      avg_battery = Enum.reduce(readings_list, 0, fn r, acc -> 
        acc + r.value.battery_level 
      end) / length(readings_list)

      status = cond do
        avg_battery < 10 -> :low_battery
        length(readings_list) < 5 -> :poor_connectivity  # Expected more readings
        true -> :healthy
      end

      health = %{
        status: status,
        last_reading: latest_reading.value.timestamp,
        reading_count: length(readings_list),
        battery_level: avg_battery
      }

      {device_id, health}
    end
  end

  defp predict_maintenance_needs({device_id, readings}) do
    readings_list = if is_list(readings), do: readings, else: [readings]
    
    # Simple predictive model based on reading patterns
    reading_values = Enum.map(readings_list, fn r -> r.value.reading end)
    
    maintenance_indicators = [
      # High variance indicates instability
      variance_score(reading_values),
      # Battery degradation
      battery_degradation_score(readings_list),
      # Signal quality issues
      signal_quality_score(readings_list)
    ]

    maintenance_score = Enum.sum(maintenance_indicators) / length(maintenance_indicators)
    
    prediction = %{
      maintenance_score: maintenance_score,
      recommended_action: (if maintenance_score > 0.8, do: "immediate", else: "monitor"),
      indicators: %{
        variance: Enum.at(maintenance_indicators, 0),
        battery: Enum.at(maintenance_indicators, 1),
        signal: Enum.at(maintenance_indicators, 2)
      }
    }

    {device_id, prediction}
  end

  defp create_dashboard_update({zone, readings}) do
    readings_list = if is_list(readings), do: readings, else: [readings]
    
    by_type = Enum.group_by(readings_list, fn r -> r.value.device_type end)
    
    zone_summary = %{
      zone: zone,
      timestamp: DateTime.utc_now(),
      device_count: length(readings_list),
      metrics: Enum.map(by_type, fn {type, type_readings} ->
        values = Enum.map(type_readings, fn r -> r.value.reading end)
        %{
          type: type,
          current: List.last(values),
          average: Enum.sum(values) / length(values),
          min: Enum.min(values),
          max: Enum.max(values)
        }
      end)
    }

    {zone, zone_summary}
  end

  defp publish_to_dashboard({zone, summary}) do
    # Simulate publishing to real-time dashboard
    Logger.info("Dashboard Update - Zone #{zone}: #{length(summary.metrics)} metrics")
    
    # In a real system, this would push to WebSocket, Phoenix Channels, etc.
    TopicProducer.publish("dashboard_updates", summary, key: "zone_#{zone}")
  end

  # Helper functions for data generation and processing

  defp generate_location do
    zones = ["Zone_A", "Zone_B", "Zone_C", "Zone_D", "Zone_E"]
    %{
      zone: Enum.random(zones),
      latitude: 40.7128 + (:rand.uniform() - 0.5) * 0.1,
      longitude: -74.0060 + (:rand.uniform() - 0.5) * 0.1,
      floor: Enum.random(1..10)
    }
  end

  defp generate_normal_range do
    %{min: 0, max: 100}  # Simplified for this example
  end

  defp get_unit_for_type(type) do
    case type do
      "temperature" -> "°C"
      "humidity" -> "%"
      "pressure" -> "hPa"
      "motion" -> "binary"
      "light" -> "lux"
    end
  end

  defp validate_reading(reading) do
    # Add validation flags
    Map.put(reading, :validation, %{
      in_range: is_in_expected_range(reading),
      timestamp_valid: is_valid_timestamp(reading.timestamp)
    })
  end

  defp add_derived_metrics(reading) do
    # Add derived metrics based on reading type and value
    derived = case reading.device_type do
      "temperature" -> %{heat_index: calculate_heat_index(reading.reading)}
      "humidity" -> %{comfort_level: categorize_humidity(reading.reading)}
      _ -> %{}
    end
    
    Map.put(reading, :derived, derived)
  end

  defp add_geospatial_context(reading) do
    # Add geospatial context
    Map.put(reading, :geo_context, %{
      region: determine_region(reading.location),
      climate_zone: determine_climate_zone(reading.location)
    })
  end

  defp store_reading(_reading) do
    # Simulate storing in time-series database
    :ok
  end

  defp is_anomalous_reading(reading) do
    # Simple anomaly detection - values outside expected ranges
    case reading.device_type do
      "temperature" -> reading.reading < -10 || reading.reading > 50
      "humidity" -> reading.reading < 0 || reading.reading > 100
      "pressure" -> reading.reading < 980 || reading.reading > 1050
      _ -> false
    end
  end

  defp determine_anomaly_type(reading) do
    cond do
      reading.reading > 100 -> :high_value
      reading.reading < -100 -> :low_value
      true -> :outlier
    end
  end

  defp calculate_anomaly_severity(reading) do
    # Simple severity calculation
    case reading.device_type do
      "temperature" -> 
        if reading.reading > 40 || reading.reading < -5, do: :high, else: :medium
      _ -> :medium
    end
  end

  defp calculate_variance(%{count: count, sum: sum}) when count > 1 do
    # Simplified variance calculation
    mean = sum / count
    # In real implementation, would need individual readings for proper variance
    abs(mean - sum / count) * 100
  end
  defp calculate_variance(_), do: 0.0

  defp variance_score(values) when length(values) > 1 do
    mean = Enum.sum(values) / length(values)
    variance = Enum.sum(Enum.map(values, fn v -> (v - mean) * (v - mean) end)) / length(values)
    min(variance / 1000, 1.0)  # Normalize to 0-1
  end
  defp variance_score(_), do: 0.0

  defp battery_degradation_score(readings) do
    battery_levels = Enum.map(readings, fn r -> r.value.battery_level end)
    if length(battery_levels) > 1 do
      first = List.first(battery_levels)
      last = List.last(battery_levels)
      max(0, (first - last) / 100)  # Rate of battery drain
    else
      0.0
    end
  end

  defp signal_quality_score(readings) do
    avg_signal = Enum.reduce(readings, 0, fn r, acc -> 
      acc + r.value.signal_strength 
    end) / length(readings)
    
    # Poor signal is < -70 dBm
    if avg_signal < -70, do: (-70 - avg_signal) / 30, else: 0.0
  end

  defp schedule_maintenance(device_id, prediction) do
    # Simulate scheduling maintenance
    Logger.info("Maintenance scheduled for device #{device_id} - Priority: #{prediction.recommended_action}")
    
    TopicProducer.publish("maintenance_schedule", %{
      device_id: device_id,
      priority: prediction.recommended_action,
      score: prediction.maintenance_score,
      scheduled_date: DateTime.add(DateTime.utc_now(), 24 * 3600, :second)
    }, key: device_id)
  end

  # Simplified helper functions for completeness
  defp is_in_expected_range(_reading), do: true
  defp is_valid_timestamp(_timestamp), do: true
  defp calculate_heat_index(temp), do: temp * 1.1
  defp categorize_humidity(humidity) when humidity < 30, do: "dry"
  defp categorize_humidity(humidity) when humidity > 70, do: "humid"
  defp categorize_humidity(_), do: "comfortable"
  defp determine_region(_location), do: "north_america"
  defp determine_climate_zone(_location), do: "temperate"
end