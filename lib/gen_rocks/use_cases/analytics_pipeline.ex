defmodule GenRocks.UseCases.AnalyticsPipeline do
  @moduledoc """
  Real-time analytics pipeline use case.
  Processes user events for real-time dashboard metrics.
  """

  require Logger
  alias GenRocks.Queue.{Supervisor, TopicProducer}

  @doc """
  Sets up a real-time analytics pipeline for user events.
  """
  def setup_pipeline do
    Logger.info("Setting up Analytics Pipeline...")

    # Start topics
    {:ok, _} = Supervisor.start_topic("user_events", 8)
    {:ok, _} = Supervisor.start_topic("processed_metrics", 4)

    # Start event ingestion consumer
    ingestion_consumer = fn message, _context ->
      process_event(message.value)
      :ok
    end

    {:ok, _} = Supervisor.start_consumer_group("event_processor", "user_events", ingestion_consumer,
      partitions: [0, 1, 2, 3, 4, 5, 6, 7],
      max_demand: 50
    )

    # Start real-time aggregation flow
    spawn(fn -> run_realtime_aggregation() end)

    Logger.info("Analytics Pipeline setup completed")
    :ok
  end

  @doc """
  Simulates user events being produced.
  """
  def simulate_user_events(event_count \\ 1000) do
    Logger.info("Simulating #{event_count} user events...")

    event_types = ["page_view", "click", "purchase", "signup", "login", "logout"]
    user_pool = 1..100

    1..event_count
    |> Enum.each(fn _i ->
      event = %{
        event_type: Enum.random(event_types),
        user_id: Enum.random(user_pool),
        timestamp: DateTime.utc_now(),
        session_id: "session_#{:rand.uniform(50)}",
        properties: generate_event_properties()
      }

      TopicProducer.publish("user_events", event, key: "user_#{event.user_id}")
      
      # Random delay to simulate realistic event streaming
      if rem(event_count, 100) == 0 do
        Process.sleep(1)
      end
    end)

    Logger.info("Finished simulating #{event_count} events")
  end

  @doc """
  Runs real-time metric aggregation using Flow.
  """
  def run_realtime_aggregation do
    Logger.info("Starting real-time aggregation...")

    # Process events in 30-second windows
    GenRocks.from_topic("user_events", partition_count: 8)
    |> GenRocks.window({:fixed, 30_000})  # 30-second windows
    |> GenRocks.transform(&enrich_event/1)
    |> GenRocks.group_by_key(fn msg -> msg.value.event_type end)
    |> GenRocks.reduce(
      fn -> %{count: 0, unique_users: MapSet.new(), revenue: 0.0} end,
      fn {event_type, events}, acc ->
        events_list = if is_list(events), do: events, else: [events]
        
        unique_users = Enum.reduce(events_list, acc.unique_users, fn event, users ->
          MapSet.put(users, event.value.user_id)
        end)

        revenue = if event_type == "purchase" do
          Enum.reduce(events_list, acc.revenue, fn event, total ->
            total + (event.value.properties["amount"] || 0)
          end)
        else
          acc.revenue
        end

        %{
          count: acc.count + length(events_list),
          unique_users: unique_users,
          revenue: revenue
        }
      end
    )
    |> GenRocks.side_effect(&publish_metrics/1)
    |> GenRocks.run_to(fn metrics ->
      Logger.info("Real-time metrics: #{inspect(metrics)}")
    end)
  end

  @doc """
  Creates user behavior analysis pipeline.
  """
  def user_behavior_analysis do
    Logger.info("Running user behavior analysis...")

    # Analyze user journeys
    result = GenRocks.from_topic("user_events", partition_count: 8)
    |> GenRocks.filter(fn msg -> msg.value.event_type in ["page_view", "click", "purchase"] end)
    |> GenRocks.group_by_key(fn msg -> msg.value.user_id end)
    |> GenRocks.transform(&analyze_user_journey/1)
    |> GenRocks.filter(fn {_user_id, analysis} -> 
      analysis.conversion_probability > 0.3 
    end)
    |> GenRocks.collect()

    Logger.info("High-conversion users identified: #{length(result)}")
    result
  end

  @doc """
  Creates A/B testing analysis pipeline.
  """
  def ab_testing_analysis(test_name) do
    Logger.info("Running A/B test analysis for: #{test_name}")

    GenRocks.from_topic("user_events", partition_count: 8)
    |> GenRocks.filter(fn msg -> 
      Map.get(msg.value.properties, "ab_test") == test_name
    end)
    |> GenRocks.group_by_key(fn msg -> 
      msg.value.properties["variant"] || "control"
    end)
    |> GenRocks.reduce(
      fn -> %{conversions: 0, total_users: MapSet.new()} end,
      fn {_variant, events}, acc ->
        events_list = if is_list(events), do: events, else: [events]
        
        users = Enum.reduce(events_list, acc.total_users, fn event, users ->
          MapSet.put(users, event.value.user_id)
        end)

        conversions = Enum.count(events_list, fn event ->
          event.value.event_type == "purchase"
        end)

        %{
          conversions: acc.conversions + conversions,
          total_users: users
        }
      end
    )
    |> GenRocks.transform(fn {variant, stats} ->
      conversion_rate = if MapSet.size(stats.total_users) > 0 do
        stats.conversions / MapSet.size(stats.total_users)
      else
        0.0
      end

      %{
        variant: variant,
        users: MapSet.size(stats.total_users),
        conversions: stats.conversions,
        conversion_rate: conversion_rate
      }
    end)
    |> GenRocks.collect()
  end

  @doc """
  Fraud detection pipeline.
  """
  def fraud_detection_pipeline do
    Logger.info("Starting fraud detection pipeline...")

    GenRocks.from_topic("user_events", partition_count: 8)
    |> GenRocks.filter(fn msg -> msg.value.event_type == "purchase" end)
    |> GenRocks.window({:sliding, 60_000, 10_000})  # 1-minute sliding windows, 10s slide
    |> GenRocks.group_by_key(fn msg -> msg.value.user_id end)
    |> GenRocks.transform(&detect_fraud/1)
    |> GenRocks.filter(fn {_user_id, fraud_score} -> fraud_score > 0.7 end)
    |> GenRocks.side_effect(fn {user_id, fraud_score} ->
      Logger.warning("FRAUD ALERT: User #{user_id} has fraud score: #{fraud_score}")
      send_fraud_alert(user_id, fraud_score)
    end)
    |> GenRocks.run_to(fn alerts ->
      Logger.info("Fraud alerts processed: #{length(alerts)}")
    end)
  end

  # Private functions

  defp process_event(event) do
    # Simulate event processing (validation, enrichment, etc.)
    enriched_event = Map.put(event, :processed_at, DateTime.utc_now())
    
    # Publish to processed metrics topic for further analysis
    TopicProducer.publish("processed_metrics", enriched_event, 
      key: "metric_#{event.event_type}"
    )
  end

  defp enrich_event(msg) do
    # Simulate enrichment with user profile data, geo data, etc.
    enriched_properties = Map.merge(msg.value.properties || %{}, %{
      "geo_country" => Enum.random(["US", "UK", "CA", "DE", "FR"]),
      "device_type" => Enum.random(["mobile", "desktop", "tablet"]),
      "channel" => Enum.random(["organic", "paid", "social", "email"])
    })

    %{msg | value: %{msg.value | properties: enriched_properties}}
  end

  defp analyze_user_journey({user_id, events}) do
    events_list = if is_list(events), do: events, else: [events]
    
    page_views = Enum.count(events_list, fn e -> e.value.event_type == "page_view" end)
    clicks = Enum.count(events_list, fn e -> e.value.event_type == "click" end)
    purchases = Enum.count(events_list, fn e -> e.value.event_type == "purchase" end)
    
    # Simple conversion probability calculation
    conversion_probability = if page_views > 0 do
      (clicks * 0.3 + purchases * 0.7) / page_views
    else
      0.0
    end

    analysis = %{
      page_views: page_views,
      clicks: clicks,
      purchases: purchases,
      conversion_probability: conversion_probability,
      engagement_score: page_views + clicks * 2 + purchases * 5
    }

    {user_id, analysis}
  end

  defp detect_fraud({user_id, events}) do
    events_list = if is_list(events), do: events, else: [events]
    
    # Simple fraud detection based on purchase patterns
    purchase_count = length(events_list)
    total_amount = Enum.sum(Enum.map(events_list, fn e -> 
      e.value.properties["amount"] || 0 
    end))
    
    # Fraud indicators
    fraud_score = 0.0
    fraud_score = fraud_score + if purchase_count > 10, do: 0.3, else: 0.0
    fraud_score = fraud_score + if total_amount > 10000, do: 0.4, else: 0.0
    fraud_score = fraud_score + if purchase_count > 0 && total_amount / purchase_count > 1000, do: 0.3, else: 0.0

    {user_id, min(fraud_score, 1.0)}
  end

  defp publish_metrics({event_type, metrics}) do
    metric_data = %{
      event_type: event_type,
      count: metrics.count,
      unique_users: MapSet.size(metrics.unique_users),
      revenue: metrics.revenue,
      timestamp: DateTime.utc_now()
    }

    TopicProducer.publish("processed_metrics", metric_data, key: "aggregated_#{event_type}")
  end

  defp send_fraud_alert(user_id, fraud_score) do
    # Simulate sending alert to fraud monitoring system
    alert = %{
      user_id: user_id,
      fraud_score: fraud_score,
      alert_time: DateTime.utc_now(),
      action_required: true
    }

    TopicProducer.publish("fraud_alerts", alert, key: "fraud_#{user_id}")
  end

  defp generate_event_properties do
    %{
      "amount" => if :rand.uniform() < 0.1, do: :rand.uniform() * 1000, else: nil,
      "page_path" => "/#{Enum.random(["home", "products", "checkout", "profile", "about"])}",
      "referrer" => Enum.random(["google.com", "facebook.com", "direct", "email"]),
      "ab_test" => if :rand.uniform() < 0.5, do: "test_#{:rand.uniform(3)}", else: nil,
      "variant" => Enum.random(["control", "variant_a", "variant_b"])
    }
  end
end