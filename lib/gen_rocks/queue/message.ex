defmodule GenRocks.Queue.Message do
  @moduledoc """
  Message structure for the distributed queue system.
  Similar to Kafka messages with key, value, headers, and metadata.
  """

  @type t :: %__MODULE__{
    key: String.t() | nil,
    value: any(),
    headers: map(),
    topic: String.t(),
    partition: non_neg_integer() | nil,
    offset: non_neg_integer() | nil,
    timestamp: DateTime.t(),
    metadata: map()
  }

  defstruct [
    :key,
    :value,
    :headers,
    :topic,
    :partition,
    :offset,
    :timestamp,
    :metadata
  ]

  @doc """
  Creates a new message with the given parameters.
  """
  def new(topic, value, opts \\ []) do
    %__MODULE__{
      key: Keyword.get(opts, :key),
      value: value,
      headers: Keyword.get(opts, :headers, %{}),
      topic: topic,
      partition: Keyword.get(opts, :partition),
      offset: Keyword.get(opts, :offset),
      timestamp: Keyword.get(opts, :timestamp, DateTime.utc_now()),
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  @doc """
  Serializes a message for storage.
  """
  def serialize(message) do
    message
    |> Map.from_struct()
    |> Jason.encode!()
  end

  @doc """
  Deserializes a message from storage.
  """
  def deserialize(data) when is_binary(data) do
    data
    |> Jason.decode!(keys: :atoms)
    |> then(&struct(__MODULE__, &1))
  end

  @doc """
  Generates a partition key for routing.
  """
  def partition_key(%__MODULE__{key: key}) when is_binary(key), do: key
  def partition_key(%__MODULE__{topic: topic, timestamp: timestamp}) do
    "#{topic}_#{DateTime.to_unix(timestamp)}"
  end
end