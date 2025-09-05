defmodule GenRocks.Queue.FlowProcessor do
  @moduledoc """
  Flow-based transformation pipeline for distributed queue processing.
  Provides Apache Beam PCollection-like functionality with custom transformations.
  """

  require Logger

  @doc """
  Creates a new Flow processing pipeline from a topic.
  Similar to Apache Beam's PCollection creation.
  """
  def from_topic(topic, opts \\ []) do
    stages = Keyword.get(opts, :stages, System.schedulers_online())
    max_demand = Keyword.get(opts, :max_demand, 100)
    partition_count = Keyword.get(opts, :partition_count, 4)

    # Create a stream from all partitions
    0..(partition_count - 1)
    |> Flow.from_enumerable(stages: stages)
    |> Flow.flat_map(fn partition ->
      # Stream messages from each partition
      create_partition_stream(topic, partition, max_demand)
    end)
  end

  @doc """
  Creates a Flow from an enumerable of messages.
  Useful for testing or processing batches of messages.
  """
  def from_enumerable(messages, opts \\ []) do
    stages = Keyword.get(opts, :stages, System.schedulers_online())
    
    messages
    |> Flow.from_enumerable(stages: stages)
  end

  @doc """
  Applies a custom transformation function to each message.
  Similar to Apache Beam's ParDo transforms.
  """
  def transform(flow, transform_fn) when is_function(transform_fn, 1) do
    Flow.map(flow, transform_fn)
  end

  @doc """
  Applies a custom transformation function that can emit multiple elements.
  Similar to Apache Beam's FlatMap transforms.
  """
  def flat_transform(flow, transform_fn) when is_function(transform_fn, 1) do
    Flow.flat_map(flow, transform_fn)
  end

  @doc """
  Filters messages based on a predicate function.
  """
  def filter(flow, predicate_fn) when is_function(predicate_fn, 1) do
    Flow.filter(flow, predicate_fn)
  end

  @doc """
  Groups messages by a key extraction function.
  Similar to Apache Beam's GroupByKey transform.
  """
  def group_by_key(flow, key_fn, opts \\ []) when is_function(key_fn, 1) do
    flow
    |> Flow.partition(key: key_fn, stages: Keyword.get(opts, :stages, 4))
    |> Flow.group_by(key_fn)
  end

  @doc """
  Reduces messages within each partition using an accumulator function.
  Similar to Apache Beam's Combine transforms.
  """
  def reduce(flow, initial_acc_fn, reducer_fn) when is_function(initial_acc_fn, 0) and is_function(reducer_fn, 2) do
    Flow.reduce(flow, initial_acc_fn, reducer_fn)
  end

  @doc """
  Aggregates messages using common aggregation functions.
  """
  def aggregate(flow, aggregation_type, opts \\ []) do
    case aggregation_type do
      :count ->
        Flow.reduce(flow, fn -> 0 end, fn _msg, acc -> acc + 1 end)
      
      :sum ->
        value_fn = Keyword.get(opts, :value_fn, & &1.value)
        Flow.reduce(flow, fn -> 0 end, fn msg, acc -> acc + value_fn.(msg) end)
      
      :average ->
        value_fn = Keyword.get(opts, :value_fn, & &1.value)
        flow
        |> Flow.reduce(fn -> {0, 0} end, fn msg, {sum, count} -> 
          {sum + value_fn.(msg), count + 1} 
        end)
        |> Flow.map(fn {sum, count} -> if count > 0, do: sum / count, else: 0 end)
      
      :collect ->
        Flow.reduce(flow, fn -> [] end, fn msg, acc -> [msg | acc] end)
    end
  end

  @doc """
  Applies windowing to the flow for time-based processing.
  Similar to Apache Beam's windowing functions.
  """
  def window(flow, window_spec) do
    case window_spec do
      {:fixed, duration_ms} ->
        Flow.partition(flow, window: Flow.Window.fixed(1, :second, duration_ms), stages: 4)
      
      {:sliding, duration_ms, slide_ms} ->
        Flow.partition(flow, window: Flow.Window.fixed(slide_ms, :millisecond, duration_ms), stages: 4)
      
      {:session, gap_ms} ->
        # Session windowing is more complex, simplified implementation
        Flow.partition(flow, window: Flow.Window.fixed(1, :second, gap_ms), stages: 4)
    end
  end

  @doc """
  Combines multiple flows into a single flow.
  Similar to Apache Beam's Flatten transform.
  """
  def flatten(flows) when is_list(flows) do
    flows
    |> Enum.reduce(fn flow, acc -> Flow.merge(acc, flow) end)
  end

  @doc """
  Branches the flow into multiple parallel processing paths.
  """
  def branch(flow, branch_specs) when is_list(branch_specs) do
    Enum.map(branch_specs, fn {name, transform_fn} ->
      {name, Flow.map(flow, transform_fn)}
    end)
  end

  @doc """
  Applies side effects (logging, metrics, external calls) without modifying the flow.
  Similar to Apache Beam's side outputs.
  """
  def side_effect(flow, side_effect_fn) when is_function(side_effect_fn, 1) do
    Flow.map(flow, fn msg ->
      side_effect_fn.(msg)
      msg
    end)
  end

  @doc """
  Routes messages to different downstream processors based on conditions.
  """
  def route(flow, routing_specs) when is_list(routing_specs) do
    Enum.map(routing_specs, fn {condition_fn, processor_fn} ->
      flow
      |> Flow.filter(condition_fn)
      |> Flow.map(processor_fn)
    end)
  end

  @doc """
  Buffers messages and processes them in batches.
  """
  def batch(flow, batch_size, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5000)
    
    flow
    |> Flow.reduce(fn -> [] end, fn msg, acc -> 
      batch = [msg | acc]
      if length(batch) >= batch_size do
        # Emit batch and reset
        batch
      else
        batch
      end
    end)
  end

  @doc """
  Applies rate limiting to the flow.
  """
  def rate_limit(flow, max_per_second) do
    interval_ms = trunc(1000 / max_per_second)
    
    flow
    |> Flow.map(fn msg ->
      Process.sleep(interval_ms)
      msg
    end)
  end

  @doc """
  Enriches messages with external data.
  """
  def enrich(flow, enrichment_fn) when is_function(enrichment_fn, 1) do
    Flow.map(flow, fn msg ->
      try do
        enriched_data = enrichment_fn.(msg)
        %{msg | metadata: Map.merge(msg.metadata || %{}, %{enriched: enriched_data})}
      rescue
        error ->
          Logger.warning("Enrichment failed for message #{inspect(msg)}: #{inspect(error)}")
          msg
      end
    end)
  end

  @doc """
  Validates messages and handles errors.
  """
  def validate(flow, validation_fn) when is_function(validation_fn, 1) do
    flow
    |> Flow.map(fn msg ->
      case validation_fn.(msg) do
        :ok -> {:valid, msg}
        {:ok, modified_msg} -> {:valid, modified_msg}
        {:error, reason} -> {:invalid, msg, reason}
      end
    end)
    |> Flow.partition(key: fn 
      {:valid, _} -> :valid
      {:invalid, _, _} -> :invalid
    end)
  end

  @doc """
  Materializes the flow into a concrete result.
  """
  def collect(flow) do
    Flow.into_stages(flow, [Enum])
  end

  @doc """
  Runs the flow and sends results to a GenStage consumer or function.
  """
  def run_to(flow, destination) do
    case destination do
      pid when is_pid(pid) ->
        Flow.into_stages(flow, [pid])
      
      {module, function, args} ->
        flow
        |> Flow.map(fn result ->
          apply(module, function, [result | args])
          result
        end)
        |> Flow.run()
      
      fun when is_function(fun, 1) ->
        flow
        |> Flow.map(fn result ->
          fun.(result)
          result
        end)
        |> Flow.run()
    end
  end

  # Example composite transformations

  @doc """
  Word count transformation (classic MapReduce example).
  """
  def word_count(flow) do
    flow
    |> Flow.flat_map(fn msg -> 
      case msg.value do
        text when is_binary(text) -> String.split(text, ~r/\W+/)
        _ -> []
      end
    end)
    |> Flow.filter(&(&1 != ""))
    |> Flow.partition(key: &String.downcase/1)
    |> Flow.reduce(fn -> %{} end, fn word, acc ->
      Map.update(acc, String.downcase(word), 1, &(&1 + 1))
    end)
  end

  @doc """
  Message deduplication based on a key function.
  """
  def deduplicate(flow, key_fn, opts \\ []) do
    window_ms = Keyword.get(opts, :window_ms, 60_000)
    
    flow
    |> Flow.partition(key: key_fn)
    |> Flow.reduce(fn -> MapSet.new() end, fn msg, seen ->
      key = key_fn.(msg)
      if MapSet.member?(seen, key) do
        seen  # Drop duplicate
      else
        MapSet.put(seen, key)
      end
    end)
  end

  # Private helper functions

  defp create_partition_stream(topic, partition, max_demand) do
    # Create a stream that reads from a specific partition
    Stream.resource(
      fn -> 
        # Initialize consumer for this partition
        case GenRocks.Queue.QueueManager.start_link(topic, partition) do
          {:ok, pid} -> {pid, 0}
          {:error, {:already_started, pid}} -> {pid, 0}
        end
      end,
      fn {pid, offset} ->
        # Request messages from queue manager
        case GenStage.call(pid, {:get_messages, offset, max_demand}, 5000) do
          {:ok, messages} ->
            next_offset = if length(messages) > 0 do
              List.last(messages).offset + 1
            else
              offset
            end
            {messages, {pid, next_offset}}
          
          :not_found ->
            # No more messages, wait a bit
            Process.sleep(100)
            {[], {pid, offset}}
          
          error ->
            Logger.error("Failed to read from partition #{partition}: #{inspect(error)}")
            {[], {pid, offset}}
        end
      end,
      fn {_pid, _offset} -> :ok end
    )
  end
end