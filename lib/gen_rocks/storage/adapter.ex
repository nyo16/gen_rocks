defmodule GenRocks.Storage.Adapter do
  @moduledoc """
  Behavior for storage adapters in the distributed queue system.
  Allows pluggable storage backends (ETS, RocksDB, custom implementations).
  """

  @type storage_ref :: term()
  @type key :: String.t() | binary()
  @type value :: term()
  @type options :: keyword()

  @doc """
  Opens/initializes the storage with the given configuration.
  Returns a storage reference that will be passed to other functions.
  """
  @callback open(config :: map()) :: {:ok, storage_ref()} | {:error, term()}

  @doc """
  Closes the storage and cleans up resources.
  """
  @callback close(storage_ref()) :: :ok | {:error, term()}

  @doc """
  Stores a key-value pair.
  """
  @callback put(storage_ref(), key(), value(), options()) :: :ok | {:error, term()}

  @doc """
  Retrieves a value by key.
  """
  @callback get(storage_ref(), key(), options()) :: {:ok, value()} | :not_found | {:error, term()}

  @doc """
  Deletes a key-value pair.
  """
  @callback delete(storage_ref(), key(), options()) :: :ok | {:error, term()}

  @doc """
  Checks if a key exists.
  """
  @callback exists?(storage_ref(), key(), options()) :: boolean() | {:error, term()}

  @doc """
  Performs an atomic batch operation.
  Operations should be a list of {:put, key, value} | {:delete, key} tuples.
  """
  @callback batch(storage_ref(), operations :: list(), options()) :: :ok | {:error, term()}

  @doc """
  Iterates over keys with an optional prefix.
  Returns an enumerable of {key, value} pairs.
  """
  @callback scan(storage_ref(), prefix :: key() | nil, options()) :: Enumerable.t() | {:error, term()}

  @doc """
  Gets storage statistics/info.
  """
  @callback info(storage_ref()) :: map() | {:error, term()}

  @doc """
  Optional callback for storage-specific optimizations.
  """
  @callback optimize(storage_ref(), options()) :: :ok | {:error, term()}

  @optional_callbacks [optimize: 2]

  @doc """
  Helper function to create a storage adapter instance.
  """
  def new(adapter_module, config) do
    case adapter_module.open(config) do
      {:ok, storage_ref} -> {:ok, {adapter_module, storage_ref}}
      error -> error
    end
  end

  @doc """
  Delegates a function call to the appropriate adapter module.
  """
  def call({adapter_module, storage_ref}, function, args) do
    apply(adapter_module, function, [storage_ref | args])
  end
end