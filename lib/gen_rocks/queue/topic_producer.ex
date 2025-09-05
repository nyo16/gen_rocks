defmodule GenRocks.Queue.TopicProducer do
  @moduledoc """
  GenStage producer for topics in the distributed queue system.
  Manages message production and coordinates with partition routers.
  """

  use GenStage
  require Logger

  alias GenRocks.Queue.Message

  defstruct [
    :topic,
    :message_queue,
    :total_messages,
    :config
  ]

  @doc """
  Starts the topic producer for a given topic.
  """
  def start_link(topic, opts \\ []) do
    GenStage.start_link(__MODULE__, {topic, opts}, name: via_tuple(topic))
  end

  @doc """
  Publishes a message to the topic.
  """
  def publish(topic, value, opts \\ []) do
    message = Message.new(topic, value, opts)
    GenStage.cast(via_tuple(topic), {:publish, message})
  end

  @doc """
  Publishes multiple messages to the topic.
  """
  def publish_batch(topic, messages) do
    GenStage.cast(via_tuple(topic), {:publish_batch, messages})
  end

  @impl true
  def init({topic, opts}) do
    config = %{
      buffer_size: Keyword.get(opts, :buffer_size, 1000),
      auto_generate: Keyword.get(opts, :auto_generate, false),
      generation_rate: Keyword.get(opts, :generation_rate, 100)
    }

    state = %__MODULE__{
      topic: topic,
      message_queue: :queue.new(),
      total_messages: 0,
      config: config
    }

    if config.auto_generate do
      schedule_message_generation()
    end

    Logger.info("TopicProducer started for topic: #{topic}")
    {:producer, state}
  end

  @impl true
  def handle_demand(demand, state) when demand > 0 do
    {messages, new_queue} = dequeue_messages(state.message_queue, demand, [])
    
    new_state = %{state | message_queue: new_queue}

    Logger.debug("TopicProducer #{state.topic} fulfilling demand: #{demand}, sending: #{length(messages)}")
    
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_cast({:publish, message}, state) do
    new_queue = :queue.in(message, state.message_queue)
    new_state = %{state | 
      message_queue: new_queue,
      total_messages: state.total_messages + 1
    }
    
    Logger.debug("Message published to topic #{state.topic}: #{inspect(message.value)}")
    {:noreply, [], new_state}
  end

  @impl true
  def handle_cast({:publish_batch, messages}, state) do
    new_queue = Enum.reduce(messages, state.message_queue, fn msg, acc ->
      :queue.in(msg, acc)
    end)
    
    new_state = %{state |
      message_queue: new_queue,
      total_messages: state.total_messages + length(messages)
    }

    Logger.debug("Batch of #{length(messages)} messages published to topic #{state.topic}")
    {:noreply, [], new_state}
  end

  @impl true
  def handle_info(:generate_message, state) do
    if state.config.auto_generate do
      message = Message.new(state.topic, generate_random_message(), 
        key: "auto-#{state.total_messages}",
        metadata: %{generated: true}
      )
      
      new_queue = :queue.in(message, state.message_queue)
      new_state = %{state | 
        message_queue: new_queue,
        total_messages: state.total_messages + 1
      }
      
      schedule_message_generation()
      {:noreply, [], new_state}
    else
      {:noreply, [], state}
    end
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("TopicProducer received unknown message: #{inspect(msg)}")
    {:noreply, [], state}
  end

  # Private functions

  defp dequeue_messages(queue, 0, acc), do: {Enum.reverse(acc), queue}
  defp dequeue_messages(queue, demand, acc) do
    case :queue.out(queue) do
      {{:value, message}, new_queue} ->
        dequeue_messages(new_queue, demand - 1, [message | acc])
      {:empty, queue} ->
        {Enum.reverse(acc), queue}
    end
  end

  defp schedule_message_generation do
    Process.send_after(self(), :generate_message, 1000)
  end

  defp generate_random_message do
    %{
      id: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower),
      data: "Generated message at #{DateTime.utc_now()}",
      value: :rand.uniform(1000)
    }
  end

  defp via_tuple(topic) do
    {:via, Registry, {GenRocks.ProducerRegistry, topic}}
  end
end